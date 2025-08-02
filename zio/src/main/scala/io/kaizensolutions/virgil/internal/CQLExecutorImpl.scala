package io.kaizensolutions.virgil.internal

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchType => _, _}
import com.datastax.oss.driver.api.core.metrics.Metrics
import io.kaizensolutions.virgil._
import io.kaizensolutions.virgil.configuration.ExecutionAttributes
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.Proofs._
import zio._
import zio.stream._

import scala.jdk.CollectionConverters._

/**
 * CQLExecutorImpl is a ZIO based client for the Apache Cassandra Java Driver
 * that provides ZIO and ZStream abstractions over the Datastax Java driver. We
 * consider CQLExecutor to be the interpreter of [[CQL[A]]] expressions.
 *
 * @param underlyingSession
 *   is the underlying Datastax Java driver session
 */
private[virgil] class CQLExecutorImpl(underlyingSession: CqlSession) extends CQLExecutor {
  override def execute[A](in: CQL[A])(implicit trace: Trace): Stream[Throwable, A] =
    in.cqlType match {
      case m: CQLType.Mutation =>
        ZStream.fromZIO(executeMutation(m, in.executionAttributes).asInstanceOf[Task[A]])

      case b: CQLType.Batch =>
        ZStream.fromZIO(executeBatch(b, in.executionAttributes).asInstanceOf[Task[A]])

      case q @ CQLType.Query(_, _, pullMode) =>
        pullMode match {
          case PullMode.TakeUpto(n) =>
            executeGeneralQuery(q, in.executionAttributes).take(n)

          case PullMode.All =>
            executeGeneralQuery(q, in.executionAttributes)
        }
    }

  override def executeMutation(in: CQL[MutationResult])(implicit trace: Trace): Task[MutationResult] =
    in.cqlType match {
      case mutation: CQLType.Mutation =>
        executeMutation(mutation, in.executionAttributes)

      case batch: CQLType.Batch =>
        executeBatch(batch, in.executionAttributes)

      case CQLType.Query(_, _, _) =>
        sys.error("Cannot perform a query using executeMutation")
    }

  override def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit
    ev: A =:!= MutationResult,
    trace: Trace
  ): Task[Paged[A]] = {
    val _ = ev
    in.cqlType match {
      case _: CQLType.Mutation =>
        sys.error("Mutations cannot be used with page queries")

      case CQLType.Batch(_, _) =>
        sys.error("Batch Mutations cannot be used with page queries")

      case q @ CQLType.Query(_, _, _) =>
        fetchSinglePage(q, pageState, in.executionAttributes)
          .asInstanceOf[Task[Paged[A]]]
    }
  }

  private def fetchSinglePage[A](
    q: CQLType.Query[A],
    pageState: Option[PageState],
    attr: ExecutionAttributes
  )(implicit trace: Trace): Task[Paged[A]] = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(q)
    for {
      boundStatement        <- buildStatement(queryString, bindMarkers, attr)
      reader                 = q.reader
      driverPageState        = pageState.map(_.underlying).orNull
      boundStatementWithPage = boundStatement.setPagingState(driverPageState)
      rp                    <- selectPage(boundStatementWithPage)
      (results, nextPage)    = rp
      chunksToOutput         = results.map(reader.decode)
    } yield Paged(chunksToOutput, nextPage)
  }

  private def executeMutation(m: CQLType.Mutation, config: ExecutionAttributes)(implicit
    trace: Trace
  ): Task[MutationResult] =
    for {
      statement <- buildMutation(m, config)
      result    <- executeAction(statement)
    } yield MutationResult.make(result.wasApplied())

  private def executeBatch(m: CQLType.Batch, config: ExecutionAttributes)(implicit trace: Trace): Task[MutationResult] =
    ZIO
      .foreach(m.mutations)(buildMutation(_))
      .mapAttempt { statementsToBatch =>
        val batch = BatchStatement
          .builder(m.batchType.toDriver)
          .addStatements(statementsToBatch.toSeq.asJava)

        config.configureBatch(batch).build()
      }
      .flatMap(executeAction)
      .map(r => MutationResult.make(r.wasApplied()))

  private def executeGeneralQuery[Output](
    input: CQLType.Query[Output],
    config: ExecutionAttributes
  )(implicit trace: Trace): ZStream[Any, Throwable, Output] = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(input)
    for {
      boundStatement <- ZStream.from(buildStatement(queryString, bindMarkers, config))
      reader          = input.reader
      element        <- select(boundStatement).mapChunks(_.map(reader.decode))
    } yield element
  }

  private def buildMutation(
    in: CQLType.Mutation,
    attr: ExecutionAttributes = ExecutionAttributes.default
  )(implicit trace: Trace): Task[BatchableStatement[_]] = {
    val (queryString, bindMarkers) = CqlStatementRenderer.render(in)

    if (bindMarkers.isEmpty) ZIO.succeed(SimpleStatement.newInstance(queryString))
    else buildStatement(queryString, bindMarkers, attr)
  }

  private def prepare(query: String)(implicit trace: Trace): Task[PreparedStatement] =
    ZIO.fromCompletionStage(underlyingSession.prepareAsync(query))

  private def executeAction(query: Statement[_])(implicit trace: Trace): Task[AsyncResultSet] =
    ZIO.fromCompletionStage(underlyingSession.executeAsync(query))

  private def select(query: Statement[_])(implicit trace: Trace): ZStream[Any, Throwable, Row] = {
    def go(in: AsyncResultSet): ZChannel[Any, Any, Any, Any, Throwable, Chunk[Row], Unit] = {
      // calling in.currentPage() will mutate the data structure and change results so use in.remaining to check
      // before consuming
      val next =
        if (in.hasMorePages) ZChannel.fromZIO(ZIO.fromCompletionStage(in.fetchNextPage())).flatMap(go)
        else ZChannel.unit

      if (in.remaining() > 0) ZChannel.write(Chunk.fromIterable(in.currentPage().asScala)) *> next
      else next
    }

    ZChannel
      .fromZIO(ZIO.fromCompletionStage(underlyingSession.executeAsync(query)))
      .flatMap(go)
      .toStream
  }

  private def selectPage(
    queryConfiguredWithPageState: Statement[_]
  )(implicit trace: Trace): Task[(Chunk[Row], Option[PageState])] =
    executeAction(queryConfiguredWithPageState).map { rs =>
      val currentRows = Chunk.fromIterable(rs.currentPage().asScala)
      if (rs.hasMorePages) {
        val pageState = PageState.fromDriver(rs.getExecutionInfo.getSafePagingState)
        (currentRows, Option(pageState))
      } else (currentRows, None)
    }

  private def buildStatement(
    queryString: String,
    columns: BindMarkers,
    config: ExecutionAttributes
  )(implicit trace: Trace): Task[BoundStatement] =
    prepare(queryString).mapAttempt { preparedStatement =>
      val result: BoundStatementBuilder = {
        val initial      = preparedStatement.boundStatementBuilder()
        val boundColumns = columns.underlying.foldLeft(initial) { case (accBuilder, (colName, column)) =>
          column.write.encodeByFieldName(
            structure = accBuilder,
            fieldName = colName.name,
            value = column.value
          )
        }
        // Configure bound statement with the common execution attributes
        // https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/statements/
        config.configure(boundColumns)
      }
      result.build()
    }

  override def metrics: UIO[Option[Metrics]] = ZIO.succeed {
    val underlyingMetrics = underlyingSession.getMetrics
    if (underlyingMetrics.isPresent) Some(underlyingMetrics.get) else None
  }
}
