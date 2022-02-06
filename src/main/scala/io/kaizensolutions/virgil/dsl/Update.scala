package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.Writer
import io.kaizensolutions.virgil.{Action, Column, ColumnName, Columns}
import zio.Chunk

class Update[State <: UpdateState](
  private val table: String,
  private val columnsToSet: Columns,
  private val relations: Chunk[Relation]
) {
  def set[A](columnName: String, inputValue: A)(implicit
    ev: Writer[A],
    state: UpdateState.ColumnSet <:< State
  ): Update[UpdateState.ColumnSet] = {
    val _      = state
    val name   = ColumnName.make(columnName)
    val column = Column.make(name, inputValue)(ev)
    new Update(table, columnsToSet + column, relations)
  }

  def where(relation: Relation)(implicit ev: State =:= UpdateState.ColumnSet): Update[UpdateState.Where] = {
    val _ = ev
    new Update(table, columnsToSet, relations :+ relation)
  }

  def and(relation: Relation)(implicit ev: State =:= UpdateState.Where): Update[UpdateState.Where] = {
    val _ = ev
    new Update(table, columnsToSet, relations :+ relation)
  }

  def build(implicit ev: State =:= UpdateState.Where): Action.Single = {
    val _                        = ev
    val (assignExpr, assignData) = renderAssignments
    val (relExpr, relData)       = Relation.renderRelations(relations)
    val data                     = assignData ++ relData

    val queryString = s"UPDATE $table $assignExpr $relExpr"
    Action.Single(
      query = queryString,
      columns = data
    )
  }

  def renderAction: String = {
    val (assignExpr, _) = renderAssignments
    val (relExpr, _)    = Relation.renderRelations(relations)
    s"UPDATE $table $assignExpr $relExpr"
  }

  override def toString: String = {
    val (_, assignData) = renderAssignments
    val (_, relData)    = Relation.renderRelations(relations)
    val data            = assignData ++ relData

    s"""Update(
       |  query = $renderAction
       |  table = $table
       |  data = $data
       |)""".stripMargin
  }

  def renderAssignments: (String, Columns) = {
    val initial = (Chunk[String](), Columns.empty)
    val (assignExprChunk, columns) =
      columnsToSet.underlying.foldLeft(initial) {
        case ((accAssignmentQuery, accColumns), (columnName, column: Column)) =>
          val name           = columnName.name
          val param          = s"${name}_assignment"
          val adjustedColumn = Column.withName(ColumnName.make(param), column)

          // For example, set("col1", val1) becomes
          // "col1 = :col1_assignment" along with Columns("col1_assignment" -> val1 with write capabilities)
          val updatedAccAssignmentQuery = accAssignmentQuery :+ s"$name = :$param"
          val updatedAccColumns         = accColumns + adjustedColumn

          (updatedAccAssignmentQuery, updatedAccColumns)
      }
    val expression = "SET " + assignExprChunk.mkString(", ")
    (expression, columns)
  }
}

object Update {
  def apply(tableName: String): Update[UpdateState.Empty] =
    new Update(tableName, Columns.empty, Chunk.empty)
}

sealed trait UpdateState
object UpdateState {
  sealed trait Empty     extends UpdateState
  sealed trait ColumnSet extends Empty
  sealed trait Where     extends ColumnSet
}
