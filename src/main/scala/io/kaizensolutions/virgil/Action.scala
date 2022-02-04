package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Reader
import zio.NonEmptyChunk

sealed trait Action
object Action {

  /**
   * Single actions map 1:1 with CQL INSERT and UPDATE statements
   * @param query
   *   is the raw formulated query using named markers if there is data to
   *   submit
   * @param columns
   *   is the data needed to submit to Cassandra
   */
  final case class Single(query: String, columns: Columns) extends Action { self =>

    /**
     * If you somehow accidentally formulate a query as an action, you can use
     * toQuery to turn it back to a Query provided you have the capability to
     * read the output
     *
     * @param evidence
     *   is the capability to read the output from the database
     * @tparam OutputType
     *   is the type of the output
     * @return
     */
    def toQuery[OutputType](implicit evidence: Reader[OutputType]): Query[OutputType] =
      Query(query, columns, evidence)

    def ++(other: Single): Batch =
      Batch(NonEmptyChunk(self, other), CassandraBatchType.Logged)
  }

  /**
   * Batch Actions map 1:1 with CQL BATCH (LOGGED/UNLOGGED/COUNTER) INSERT and
   * UPDATE statements
   * @param actions
   *   are the individual actions that need to be executed as part of the batch
   */
  final case class Batch(actions: NonEmptyChunk[Single], batchType: CassandraBatchType) extends Action {
    def add(other: Single): Batch =
      Batch(actions :+ other, batchType)

    def ++(other: Batch): Batch =
      Batch(actions ++ other.actions, batchType)

    def withBatchType(in: CassandraBatchType): Batch =
      Batch(actions, in)
  }
  object Batch {
    def logged(actions: NonEmptyChunk[Single]): Batch   = Batch(actions, CassandraBatchType.Logged)
    def logged(action: Single, actions: Single*): Batch = logged(NonEmptyChunk(action, actions: _*))

    def unlogged(actions: NonEmptyChunk[Single]): Batch   = Batch(actions, CassandraBatchType.Unlogged)
    def unlogged(action: Single, actions: Single*): Batch = logged(NonEmptyChunk(action, actions: _*))

    def counter(actions: NonEmptyChunk[Single]): Batch   = Batch(actions, CassandraBatchType.Counter)
    def counter(action: Single, actions: Single*): Batch = counter(NonEmptyChunk(action, actions: _*))
  }
}
