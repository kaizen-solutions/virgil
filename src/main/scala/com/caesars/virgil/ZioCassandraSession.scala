package com.caesars.virgil

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, PreparedStatement, Row, Statement}
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
    ZStream.fromEffect(prepare(input.query)).flatMap { preparedStatement =>
      val boundStatement = {
        val initial = preparedStatement.bind()
        input.data.foldLeft(initial) { case (acc, (name, column: ValueInCql)) =>
          val writer = column.writer
          val value  = column.value
          writer.write(boundStatement = acc, column = name, value = value)
        }
      }
      val reader = input.reader
      select(boundStatement).map(reader.read("unused", _))
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
