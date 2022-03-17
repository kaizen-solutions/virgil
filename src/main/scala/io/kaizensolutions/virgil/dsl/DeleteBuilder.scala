package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.CQLType.Mutation.Delete
import io.kaizensolutions.virgil.{dsl, CQL, MutationResult}
import zio.{Chunk, NonEmptyChunk}

final case class DeleteBuilder[State <: DeleteState](
  private val tableName: String,
  private val criteria: Delete.DeleteCriteria,
  private val relations: Chunk[Relation],
  private val conditions: DeleteConditions
) {
  def entireRow(implicit ev: State =:= DeleteState.Empty): DeleteBuilder[DeleteState.CriteriaSet] = {
    val _ = ev
    copy(criteria = Delete.DeleteCriteria.EntireRow)
  }

  def column(columnName: String)(implicit
    ev: DeleteState.CriteriaSet <:< State
  ): DeleteBuilder[DeleteState.CriteriaSet] = {
    val _ = ev
    copy(criteria = addColumn(columnName))
  }

  def columns(columnName: String, columnNames: String*)(implicit
    ev: DeleteState.CriteriaSet <:< State
  ): DeleteBuilder[DeleteState.CriteriaSet] =
    columns(NonEmptyChunk(columnName, columnNames: _*))(ev)

  def columns(in: NonEmptyChunk[String])(implicit
    ev: DeleteState.CriteriaSet <:< State
  ): DeleteBuilder[DeleteState.CriteriaSet] = {
    val _ = ev
    copy(criteria = addColumns(in))
  }

  def where(
    relation: Relation
  )(implicit ev: State =:= dsl.DeleteState.CriteriaSet): DeleteBuilder[DeleteState.Where] = {
    val _ = ev
    DeleteBuilder[DeleteState.Where](
      tableName = tableName,
      criteria = criteria,
      relations = relations :+ relation,
      conditions = conditions
    )
  }

  def and(relation: Relation)(implicit ev: State =:= DeleteState.Where): DeleteBuilder[DeleteState.Where] = {
    val _ = ev
    DeleteBuilder[DeleteState.Where](
      tableName = tableName,
      criteria = criteria,
      relations = relations :+ relation,
      conditions = conditions
    )
  }

  def ifCondition(
    condition: Relation
  )(implicit ev: State =:= DeleteState.Where): DeleteBuilder[DeleteState.IfConditions] = {
    val _ = ev
    DeleteBuilder(tableName, criteria, relations, addIfCondition(condition))
  }

  def andIfCondition(
    condition: Relation
  )(implicit ev: State =:= DeleteState.IfConditions): DeleteBuilder[DeleteState.IfConditions] = {
    val _ = ev
    DeleteBuilder(tableName, criteria, relations, addIfCondition(condition))
  }

  def build(implicit ev: State <:< DeleteState.Where): CQL[MutationResult] = {
    val _              = ev
    val readyRelations = NonEmptyChunk.fromChunk(relations)

    CQL.delete(tableName = tableName, criteria = criteria, relations = readyRelations.get, conditions = conditions)
  }

  private def addColumn(in: String): Delete.DeleteCriteria =
    criteria match {
      case Delete.DeleteCriteria.EntireRow        => Delete.DeleteCriteria.Columns(NonEmptyChunk.single(in))
      case Delete.DeleteCriteria.Columns(columns) => Delete.DeleteCriteria.Columns(columns :+ in)
    }

  private def addColumns(in: NonEmptyChunk[String]): Delete.DeleteCriteria =
    criteria match {
      case Delete.DeleteCriteria.EntireRow        => Delete.DeleteCriteria.Columns(in)
      case Delete.DeleteCriteria.Columns(columns) => Delete.DeleteCriteria.Columns(columns ++ in)
    }

  private def addIfCondition(condition: Relation): DeleteConditions =
    conditions match {
      case Conditions.NoConditions             => Conditions.IfConditions(NonEmptyChunk.single(condition))
      case Conditions.IfExists                 => Conditions.IfConditions(NonEmptyChunk.single(condition))
      case Conditions.IfConditions(conditions) => Conditions.IfConditions(conditions :+ condition)
    }
}
object DeleteBuilder {
  def apply(tableName: String): DeleteBuilder[DeleteState.Empty] =
    DeleteBuilder(
      tableName = tableName,
      criteria = Delete.DeleteCriteria.EntireRow,
      Chunk.empty,
      Conditions.NoConditions
    )
}

sealed trait DeleteState
object DeleteState {
  sealed trait Empty        extends DeleteState
  sealed trait CriteriaSet  extends Empty
  sealed trait Where        extends CriteriaSet
  sealed trait IfExists     extends Where
  sealed trait IfConditions extends Where
}
