package io.kaizensolutions.virgil.internal

import cats.effect._
import cats.syntax.all._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.oss.driver.api.core.cql.BatchStatement
import com.datastax.oss.driver.api.core.cql.BatchableStatement
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.SimpleStatement
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.core.cql.{BatchType => _}
import com.datastax.oss.driver.api.core.metrics.Metrics
import fs2._
import io.kaizensolutions.virgil._
import io.kaizensolutions.virgil.configuration.ExecutionAttributes
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.Proofs._

import scala.jdk.CollectionConverters._

/**
 * CQLExecutorImpl is a ZIO based client for the Apache Cassandra Java Driver
 * that provides ZIO and ZStream abstractions over the Datastax Java driver. We
 * consider CQLExecutor to be the interpreter of [[CQL[A]]] expressions.
 *
 * @param underlyingSession
 *   is the underlying Datastax Java driver session
 */
private[virgil] class CQLExecutorImpl[F[_]](underlyingSession: CqlSession)(implicit F: Async[F])
    extends CQLExecutor[F] {
  override def execute[A](in: CQL[A]): Stream[F, A] = in.cqlType match {
    case m: CQLType.Mutation => Stream.eval(executeMutation(m, in.executionAttributes).asInstanceOf[F[A]])

    case b: CQLType.Batch => Stream.eval(executeBatch(b, in.executionAttributes).asInstanceOf[F[A]])

    case q @ CQLType.Query(_, _, pullMode) =>
      pullMode match {

        case PullMode.TakeUpto(n) => executeGeneralQuery(q, in.executionAttributes).take(n)

        case PullMode.All => executeGeneralQuery(q, in.executionAttributes)
      }
  }

  override def executeMutation(in: CQL[MutationResult]): F[MutationResult] = in.cqlType match {
    case mutation: CQLType.Mutation => executeMutation(mutation, in.executionAttributes)

    case batch: CQLType.Batch => executeBatch(batch, in.executionAttributes)

    case CQLType.Query(_, _, _) =>
      sys.error("Cannot perform a query using executeMutation")
  }

  override def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit
    ev: A =:!= MutationResult
  ): F[Paged[A]] = in.cqlType match {
    case _: CQLType.Mutation =>
      sys.error("Mutations cannot be used with page queries")
    case CQLType.Batch(_, _) =>
      sys.error("Batch Mutations cannot be used with page queries")
    case q @ CQLType.Query(_, _, _) => fetchSinglePage(q, pageState, in.executionAttributes).asInstanceOf[F[Paged[A]]]
  }

  override def metrics: F[Option[Metrics]] = F.delay {
    val underlyingMetrics = underlyingSession.getMetrics
    if (underlyingMetrics.isPresent) Some(underlyingMetrics.get) else None
  }

  private def executeMutation(m: CQLType.Mutation, config: ExecutionAttributes): F[MutationResult] =
    for {
      boundStatement <- buildMutation(m, config)
      result         <- executeAction(boundStatement)
    } yield MutationResult.make(result.wasApplied())

  private def executeBatch(m: CQLType.Batch, config: ExecutionAttributes): F[MutationResult] =
    Chunk
      .indexedSeq(m.mutations)
      .traverse(buildMutation(_))
      .flatMap { statements =>
        F.delay {
          val batch = BatchStatement
            .builder(m.batchType.toDriver)
            .addStatements(statements.to(Seq).asJava)

          config.configureBatch(batch).build()
        }
      }
      .flatMap(executeAction)
      .map(r => MutationResult.make(r.wasApplied()))

  private def buildMutation(
    in: CQLType.Mutation,
    attr: ExecutionAttributes = ExecutionAttributes.default
  ): F[BatchableStatement[_]] = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(in)

    if (bindMarkers.isEmpty) F.pure(SimpleStatement.newInstance(queryString))
    else buildStatement(queryString, bindMarkers, attr).asInstanceOf[F[BatchableStatement[_]]]
  }

  private def executeGeneralQuery[Output](input: CQLType.Query[Output], config: ExecutionAttributes) = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(input)
    for {
      boundStatement <- Stream.eval(buildStatement(queryString, bindMarkers, config))
      reader          = input.reader
      element        <- select(boundStatement).mapChunks(chunk => chunk.map(reader.decode))
    } yield element
  }

  private def select(query: Statement[_]): Stream[F, Row] = {
    def go(in: AsyncResultSet): Pull[F, Row, Unit] = {
      val next =
        if (in.hasMorePages) Pull.eval(F.fromCompletionStage(F.delay(in.fetchNextPage))).flatMap(go)
        else Pull.done

      if (in.remaining() > 0) Pull.output(Chunk.iterable(in.currentPage().asScala)) >> next
      else next
    }

    Pull
      .eval(F.fromCompletionStage(F.delay(underlyingSession.executeAsync(query))))
      .flatMap(go)
      .stream

  }

  private def fetchSinglePage[A](
    q: CQLType.Query[A],
    pageState: Option[PageState],
    attr: ExecutionAttributes
  ): F[Paged[A]] = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(q)
    for {
      boundStatement        <- buildStatement(queryString, bindMarkers, attr)
      reader                 = q.reader
      driverPageState        = pageState.map(_.underlying).orNull
      boundStatementWithPage = boundStatement.setPagingState(driverPageState)
      rp                    <- selectPage(boundStatementWithPage)
      (results, nextPage)    = rp
    } yield Paged(results.map(reader.decode), nextPage)
  }

  private def buildStatement(
    queryString: String,
    columns: BindMarkers,
    config: ExecutionAttributes
  ): F[BoundStatement] = prepare(queryString).flatMap { preparedStatement =>
    F.delay {
      val result: BoundStatementBuilder = {
        val initial = preparedStatement.boundStatementBuilder()
        val boundColumns = columns.underlying.foldLeft(initial) { case (accBuilder, (colName, column)) =>
          column.write.encodeByFieldName(
            structure = accBuilder,
            fieldName = colName.name,
            value = column.value
          )
        }

        config.configure(boundColumns)
      }
      result.build()
    }
  }

  private def selectPage(queryConfiguredWithPageState: Statement[_]): F[(Chunk[Row], Option[PageState])] =
    executeAction(queryConfiguredWithPageState).map { resultSet =>
      val rows = Chunk.iterable(resultSet.currentPage().asScala)
      if (resultSet.hasMorePages) {
        val pageState = PageState.fromDriver(resultSet.getExecutionInfo.getSafePagingState)
        (rows, Option(pageState))
      } else (rows, None)
    }

  private def executeAction(query: Statement[_]): F[AsyncResultSet] =
    F.fromCompletionStage(F.delay(underlyingSession.executeAsync(query)))

  private def prepare(query: String): F[PreparedStatement] =
    F.fromCompletionStage(F.delay(underlyingSession.prepareAsync(query)))

}
