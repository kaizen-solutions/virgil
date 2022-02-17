package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.{CQL, MutationResult}
import zio.{Chunk, NonEmptyChunk}

class UpdateBuilder[State <: UpdateState](
  private val table: String,
  private val assignments: Chunk[Assignment],
  private val relations: Chunk[Relation]
) {
  def set[A](assignment: Assignment)(implicit
    ev: UpdateState.ColumnSet <:< State
  ): UpdateBuilder[UpdateState.ColumnSet] = {
    val _ = ev
    new UpdateBuilder(table, assignments :+ assignment, relations)
  }

  def where(relation: Relation)(implicit ev: State =:= UpdateState.ColumnSet): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations :+ relation)
  }

  def and(relation: Relation)(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    new UpdateBuilder(table, assignments, relations :+ relation)
  }

  def build(implicit ev: State =:= UpdateState.Where): CQL[MutationResult] = {
    val _                = ev
    val readyAssignments = NonEmptyChunk.fromChunk(assignments)
    val readyRelations   = NonEmptyChunk.fromChunk(relations)

    CQL.update(
      tableName = table,
      assignments = readyAssignments.get,
      relations = readyRelations.get
    )
  }
}

object UpdateBuilder {
  def apply(tableName: String): UpdateBuilder[UpdateState.Empty] =
    new UpdateBuilder(tableName, Chunk.empty, Chunk.empty)
}

sealed trait UpdateState
object UpdateState {
  sealed trait Empty     extends UpdateState
  sealed trait ColumnSet extends Empty
  sealed trait Where     extends ColumnSet
}
