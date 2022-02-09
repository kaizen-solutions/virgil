package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.Mutation.{Delete, Insert, RawCql, Truncate, Update}
import io.kaizensolutions.virgil.dsl.{Assignment, InsertBuilder, InsertState, Relation, UpdateBuilder, UpdateState}
import zio.NonEmptyChunk

sealed trait Mutation
object Mutation {
  final case class Insert(tableName: String, columns: BindMarkers) extends Mutation
  object Insert {
    def into(tableName: String): InsertBuilder[InsertState.Empty] = InsertBuilder(tableName)
  }

  final case class Update(
    tableName: String,
    assignments: NonEmptyChunk[Assignment],
    relations: NonEmptyChunk[Relation]
  ) extends Mutation {
    def apply(tableName: String): UpdateBuilder[UpdateState.Empty] = UpdateBuilder(tableName)
  }

  final case class Delete(
    tableName: String,
    relations: NonEmptyChunk[Relation]
  ) extends Mutation

  final case class Truncate(tableName: String) extends Mutation

  final private[virgil] case class RawCql private (queryString: String, bindMarkers: BindMarkers) extends Mutation
}

final case class Batch private (actions: NonEmptyChunk[Mutation], batchType: BatchType) {
  def +(other: Insert): Batch             = Batch(actions :+ other, batchType)
  def +(other: Update): Batch             = Batch(actions :+ other, batchType)
  def +(other: Delete): Batch             = Batch(actions :+ other, batchType)
  def +(other: Truncate): Batch           = Batch(actions :+ other, batchType)
  def +(other: RawCql): Batch             = Batch(actions :+ other, batchType)
  def ++(other: Batch): Batch             = Batch(actions ++ other.actions, batchType)
  def withBatchType(in: BatchType): Batch = Batch(actions, in)
}
object Batch {
  def logged(mutations: NonEmptyChunk[Mutation]): Batch       = Batch(mutations, BatchType.Logged)
  def logged(mutation: Mutation, mutations: Mutation*): Batch = logged(NonEmptyChunk(mutation, mutations: _*))

  def unlogged(mutations: NonEmptyChunk[Mutation]): Batch       = Batch(mutations, BatchType.Unlogged)
  def unlogged(mutation: Mutation, mutations: Mutation*): Batch = logged(NonEmptyChunk(mutation, mutations: _*))

  def counter(mutations: NonEmptyChunk[Mutation]): Batch       = Batch(mutations, BatchType.Counter)
  def counter(mutation: Mutation, mutations: Mutation*): Batch = counter(NonEmptyChunk(mutation, mutations: _*))
}

sealed trait BatchType
object BatchType {
  case object Logged   extends BatchType
  case object Unlogged extends BatchType
  case object Counter  extends BatchType
}
