package com.caesars.virgil

import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import zio.stream.ZStream
import zio.{Chunk, Task, TaskManaged, ZIO, ZManaged}

import scala.jdk.CollectionConverters._

/**
 * ZioCassandraSession is a ZIO based wrapper for the Apache Cassandra Java
 * Driver that provides ZIO and ZStream abstractions over the Datastax Java
 * driver
 * @param session
 *   is the underlying Datastax Java driver session
 */
class ZioCassandraSession(session: CqlSession) {
  def select[Output](input: CassandraInteraction.Query[Output]): ZStream[Any, Throwable, Output] =
    ZStream.fromEffect(buildStatement(input.query, input.columns)).flatMap { boundStatement =>
      val reader = input.reader
      select(boundStatement).map(reader.read("unused", _))
    }

  def selectFirst[Output](input: CassandraInteraction.Query[Output]): Task[Option[Output]] =
    buildStatement(input.query, input.columns).flatMap { boundStatement =>
      val reader = input.reader
      selectFirst(boundStatement).map(_.map(reader.read("unused", _)))
    }

  def executeAction(input: CassandraInteraction.Action): Task[Boolean] =
    buildStatement(input.query, input.columns).flatMap { boundStatement =>
      executeAction(boundStatement)
        .map(_.wasApplied())
    }

  def prepare(query: String): Task[PreparedStatement] =
    ZIO.fromCompletionStage(session.prepareAsync(query))

  def executeAction(query: String): Task[AsyncResultSet] =
    ZIO.fromCompletionStage(session.executeAsync(query))

  def executeAction(query: Statement[_]): Task[AsyncResultSet] =
    ZIO.fromCompletionStage(session.executeAsync(query))

  def select(query: Statement[_]): ZStream[Any, Throwable, Row] = {
    val initialEffect = ZIO.fromCompletionStage(session.executeAsync(query))

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

  def selectFirst(query: Statement[_]): Task[Option[Row]] =
    executeAction(query)
      .map(resultSet => Option(resultSet.one()))

  private def buildStatement(queryString: String, columns: Columns): Task[BoundStatement] =
    prepare(queryString).mapEffect { preparedStatement =>
      val result: BoundStatementBuilder = {
        val initial = preparedStatement.boundStatementBuilder()
        columns.underlying.foldLeft(initial) { case (accBoundStatement, (colName, column)) =>
          column.write.write(
            builder = accBoundStatement,
            column = colName.name,
            value = column.value
          )
        }
      }
      result.build
    }
}

object ZioCassandraSession {

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
