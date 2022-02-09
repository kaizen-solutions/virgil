package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.{BatchType => _, _}
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import io.kaizensolutions.virgil.configuration.{ExecutionAttributes, PageState}
import io.kaizensolutions.virgil.interpreters.CqlStatement
import zio._
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

/**
 * ZioCassandraSession is a ZIO based wrapper for the Apache Cassandra Java
 * Driver that provides ZIO and ZStream abstractions over the Datastax Java
 * driver. We consider ZioCassandraSession to be the interpreter of [[Query]]s
 * and [[Mutation]]s built by the cql API or the higher level APIs that are
 * provided by the [[dsl]] package.
 *
 * @param underlyingSession
 *   is the underlying Datastax Java driver session
 */
class ZioCassandraSession(underlyingSession: CqlSession) {
  def select[Output](
    input: Query[Output],
    config: ExecutionAttributes = ExecutionAttributes.default
  ): ZStream[Any, Throwable, Output] = {
    val (queryString, bindMarkers) = CqlStatement.render(input)
    for {
      boundStatement <- ZStream.fromEffect(buildStatement(queryString, bindMarkers, config))
      reader          = input.reader
      element        <- select(boundStatement).map(reader.read("unused", _))
    } yield element
  }

  def selectFirst[Output](
    input: Query[Output],
    config: ExecutionAttributes = ExecutionAttributes.default
  ): Task[Option[Output]] = {
    val (queryString, bindMarkers) = CqlStatement.render(input)
    for {
      boundStatement <- buildStatement(queryString, bindMarkers, config)
      reader          = input.reader
      element        <- selectFirst(boundStatement).map(_.map(reader.read("unused", _)))
    } yield element
  }

  def selectPage[Output](
    input: Query[Output],
    page: Option[PageState] = None,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): Task[(Chunk[Output], Option[PageState])] = {
    val (queryString, bindMarkers) = CqlStatement.render(input)
    for {
      boundStatement        <- buildStatement(queryString, bindMarkers, config)
      reader                 = input.reader
      driverPageState        = page.map(_.underlying).orNull
      boundStatementWithPage = boundStatement.setPagingState(driverPageState)
      rp                    <- selectPage(boundStatementWithPage)
      (results, nextPage)    = rp
      chunksToOutput         = results.map(reader.read("unused", _))
    } yield (chunksToOutput, nextPage)
  }

  def execute(
    input: Mutation,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): Task[Boolean] = {
    val (queryString, bindMarkers) = CqlStatement.render(input)
    buildStatement(queryString = queryString, columns = bindMarkers, config = config)
      .flatMap(executeAction)
      .map(_.wasApplied())
  }

  def executeBatch(
    input: Batch,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): Task[Boolean] = {
    val batchType = input.batchType match {
      case BatchType.Logged   => DefaultBatchType.LOGGED
      case BatchType.Unlogged => DefaultBatchType.UNLOGGED
      case BatchType.Counter  => DefaultBatchType.COUNTER
    }

    val initial = BatchStatement.builder(batchType)
    ZIO
      .foldLeft(input.actions)(initial) { (acc, nextAction) =>
        val (nextQueryString, nextBindMarkers) = CqlStatement.render(nextAction)
        buildStatement(nextQueryString, nextBindMarkers, ExecutionAttributes.default)
          .map(boundStatement => acc.addStatement(boundStatement))
      }
      .map(config.configureBatch)
      .mapEffect(_.build())
      .flatMap(executeAction)
      .map(_.wasApplied())
  }

  def executeAction(query: String): Task[AsyncResultSet] =
    ZIO.fromCompletionStage(underlyingSession.executeAsync(query))

  private def prepare(query: String): Task[PreparedStatement] =
    ZIO.fromCompletionStage(underlyingSession.prepareAsync(query))

  private def executeAction(query: Statement[_]): Task[AsyncResultSet] =
    ZIO.fromCompletionStage(underlyingSession.executeAsync(query))

  private def select(query: Statement[_]): ZStream[Any, Throwable, Row] = {
    val initialEffect = ZIO.fromCompletionStage(underlyingSession.executeAsync(query))

    ZStream.fromEffect(initialEffect).flatMap { initial =>
      ZStream.paginateChunkM(initial) { resultSet =>
        val emit = Chunk.fromIterable(resultSet.currentPage().asScala)
        if (resultSet.hasMorePages) {
          ZIO
            .fromCompletionStage(resultSet.fetchNextPage())
            .map(nextState => emit -> Option(nextState))
        } else {
          ZIO.succeed(emit -> None)
        }
      }
    }
  }

  private def selectPage(queryConfiguredWithPageState: Statement[_]): Task[(Chunk[Row], Option[PageState])] =
    executeAction(queryConfiguredWithPageState).map { rs =>
      val currentRows = Chunk.fromIterable(rs.currentPage().asScala)
      if (rs.hasMorePages) {
        val pageState = PageState.fromDriver(rs.getExecutionInfo.getSafePagingState)
        (currentRows, Option(pageState))
      } else (currentRows, None)
    }

  private def selectFirst(query: Statement[_]): Task[Option[Row]] =
    executeAction(query)
      .map(resultSet => Option(resultSet.one()))

  private def buildStatement(
    queryString: String,
    columns: BindMarkers,
    config: ExecutionAttributes
  ): Task[BoundStatement] =
    prepare(queryString).mapEffect { preparedStatement =>
      val result: BoundStatementBuilder = {
        val initial = preparedStatement.boundStatementBuilder()
        val boundColumns = columns.underlying.foldLeft(initial) { case (accBuilder, (colName, column)) =>
          column.write.write(
            builder = accBuilder,
            column = colName.name,
            value = column.value
          )
        }
        // Configure bound statement with the common execution attributes
        // https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/statements/
        config.configure(boundColumns)
      }
      result.build()
    }
}

object ZioCassandraSession {
  def select[Output](
    input: Query[Output],
    config: ExecutionAttributes = ExecutionAttributes.default
  ): ZStream[Has[ZioCassandraSession], Throwable, Output] =
    ZStream
      .service[ZioCassandraSession]
      .flatMap(_.select(input, config))

  def selectFirst[Output](
    input: Query[Output],
    config: ExecutionAttributes = ExecutionAttributes.default
  ): RIO[Has[ZioCassandraSession], Option[Output]] =
    ZIO.serviceWith[ZioCassandraSession](_.selectFirst(input, config))

  def selectPage[Output](
    input: Query[Output],
    page: Option[PageState] = None,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): RIO[Has[ZioCassandraSession], (Chunk[Output], Option[PageState])] =
    ZIO.serviceWith[ZioCassandraSession](_.selectPage(input, page, config))

  def execute(
    input: Mutation,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): RIO[Has[ZioCassandraSession], Boolean] =
    ZIO.serviceWith[ZioCassandraSession](_.execute(input, config))

  def executeBatch(
    input: Batch,
    config: ExecutionAttributes = ExecutionAttributes.default
  ): RIO[Has[ZioCassandraSession], Boolean] =
    ZIO.serviceWith[ZioCassandraSession](_.executeBatch(input, config))

  def execute(action: String): RIO[Has[ZioCassandraSession], AsyncResultSet] =
    ZIO.serviceWith[ZioCassandraSession](_.executeAction(action))

  /**
   * Create a ZIO Cassandra Session from an existing Datastax Java Driver's
   * CqlSession Note that the user is responsible for the lifecycle of the
   * underlying CqlSession
   * @param session
   *   is the underlying Datastax Java Driver's CqlSession
   * @return
   *   the ZIO Cassandra Session
   */
  def fromExisting(session: CqlSession): ZioCassandraSession =
    new ZioCassandraSession(session)

  def apply(builder: CqlSessionBuilder): TaskManaged[ZioCassandraSession] = {
    val acquire = Task.effect(builder.build())
    val release = (session: CqlSession) => ZIO(session.close()).ignore

    ZManaged
      .make(acquire)(release)
      .map(new ZioCassandraSession(_))
  }
}
