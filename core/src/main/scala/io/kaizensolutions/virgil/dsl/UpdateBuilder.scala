package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.{CQL, MutationResult}

final case class UpdateBuilder[State <: UpdateState](
  private val table: String,
  private val assignments: IndexedSeq[Assignment],
  private val relations: IndexedSeq[Relation],
  private val conditions: UpdateConditions
) {
  def set[A](assignment: Assignment)(implicit
    ev: UpdateState.ColumnSet <:< State
  ): UpdateBuilder[UpdateState.ColumnSet] = {
    val _ = ev
    copy(assignments = assignments :+ assignment)
  }

  def where(relation: Relation)(implicit ev: State =:= UpdateState.ColumnSet): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    copy(relations = relations :+ relation)
  }

  def and(relation: Relation)(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.Where] = {
    val _ = ev
    copy(relations = relations :+ relation)
  }

  def ifExists(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.IfExists] = {
    val _ = ev
    copy(conditions = Conditions.IfExists)
  }

  def ifCondition(
    condition: Relation
  )(implicit ev: State =:= UpdateState.Where): UpdateBuilder[UpdateState.IfConditions] = {
    val _ = ev
    copy(conditions = addIfCondition(condition))
  }

  def andIfCondition(
    condition: Relation
  )(implicit ev: State =:= UpdateState.IfConditions): UpdateBuilder[UpdateState.IfConditions] = {
    val _ = ev
    copy(conditions = addIfCondition(condition))
  }

  def build(implicit ev: State <:< UpdateState.Where): CQL[MutationResult] = {
    val _ = ev

    CQL.update(
      tableName = table,
      assignments = assignments,
      relations = relations,
      updateConditions = conditions
    )
  }

  private def addIfCondition(condition: Relation): UpdateConditions =
    conditions match {
      case Conditions.NoConditions             => Conditions.IfConditions(IndexedSeq(condition))
      case Conditions.IfExists                 => Conditions.IfConditions(IndexedSeq(condition))
      case Conditions.IfConditions(conditions) => Conditions.IfConditions(conditions :+ condition)
    }
}

object UpdateBuilder {
  def apply(tableName: String): UpdateBuilder[UpdateState.Empty] =
    new UpdateBuilder(
      table = tableName,
      assignments = IndexedSeq.empty,
      relations = IndexedSeq.empty,
      conditions = Conditions.NoConditions
    )
}

sealed trait UpdateState
object UpdateState {
  sealed trait Empty        extends UpdateState
  sealed trait ColumnSet    extends Empty
  sealed trait Where        extends ColumnSet
  sealed trait IfExists     extends Where
  sealed trait IfConditions extends Where
}
