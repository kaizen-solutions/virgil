package com.caesars.virgil

import com.caesars.virgil.codecs.Reader
import zio.NonEmptyChunk

/* Models an interaction with Cassandra.
   An interaction can be a
   - a query which fetches data
   - an action which executes an insert or update
   - a batch which executes multiple actions
 */
sealed trait CassandraInteraction
object CassandraInteraction {

  /**
   * Queries map 1:1 with CQL SELECT statements and return a list of results
   * which can be viewed as any datatype having the Reader capability
   * @param query
   *   is the raw formulated query using named markers if there is data to
   *   submit
   * @param data
   *   is the data needed to submit to Cassandra
   * @param reader
   *   is the capability to read data from Cassandra
   * @tparam OutputType
   *   is the type of data we expect to read from Cassandra
   */
  final case class Query[OutputType](query: String, columns: Columns, reader: Reader[OutputType])
      extends CassandraInteraction {
    def toAction: Action = Action(query, columns)

    def withOutput[OutputType2](implicit reader: Reader[OutputType2]): Query[OutputType2] =
      Query[OutputType2](query, columns, reader)
  }
  final case class Action(query: String, columns: Columns) extends CassandraInteraction

  final case class BatchAction(actions: NonEmptyChunk[Action], batchType: CassandraBatchType)
      extends CassandraInteraction
  object BatchAction {
    def logged(actions: NonEmptyChunk[Action]): BatchAction   = BatchAction(actions, CassandraBatchType.Logged)
    def logged(action: Action, actions: Action*): BatchAction = logged(NonEmptyChunk(action, actions: _*))

    def unlogged(actions: NonEmptyChunk[Action]): BatchAction   = BatchAction(actions, CassandraBatchType.Unlogged)
    def unlogged(action: Action, actions: Action*): BatchAction = logged(NonEmptyChunk(action, actions: _*))

    def counter(actions: NonEmptyChunk[Action]): BatchAction   = BatchAction(actions, CassandraBatchType.Counter)
    def counter(action: Action, actions: Action*): BatchAction = counter(NonEmptyChunk(action, actions: _*))
  }
}
