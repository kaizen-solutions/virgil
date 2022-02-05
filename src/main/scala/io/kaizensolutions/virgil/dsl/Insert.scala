package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.Writer
import io.kaizensolutions.virgil.{Action, Column, ColumnName, Columns}

class Insert[State <: InsertState](
  private val table: String,
  private val columns: Columns,
  private val timeToLive: Option[Int] = None,
  private val timestamp: Option[Long] = None,
  private val ifNotExists: Boolean = false
) {
  def value[ScalaType](columnName: String, inputValue: ScalaType)(implicit
    ev: Writer[ScalaType]
  ): Insert[InsertState.ColumnAdded] = {
    val name   = ColumnName.make(columnName)
    val column = Column.make(name, inputValue)(ev)
    new Insert(table, columns + column)
  }

  def ifNotExists(implicit stateEvidence: State <:< InsertState.ColumnAdded): Insert[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new Insert(table = table, columns = columns, timeToLive = timeToLive, timestamp = timestamp, ifNotExists = true)
  }

  def usingTTL(timeToLive: Int)(implicit stateEvidence: State <:< InsertState.ColumnAdded): Insert[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new Insert(table, columns, Some(timeToLive))
  }

  def usingTimestamp(timestamp: Long)(implicit stateEvidence: State <:< InsertState.ColumnAdded): Insert[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new Insert(table, columns, timeToLive, Some(timestamp))
  }

  def build(implicit stateEvidence: State <:< InsertState.ColumnAdded): Action.Single = {
    // Make unused variables check happy
    val _ = stateEvidence
    Action.Single(renderQuery, columns)
  }

  override def toString: String =
    s"""Insert(
       |  query = $renderQuery,
       |  table = $table,
       |  columns = ${columns.toString},
       |)""".stripMargin

  private def renderQuery: String =
    s"INSERT INTO $table $renderColumnNames VALUES $renderInterpolatedValues $renderIfNotExists $renderTimestampTTL"

  private def renderColumnNames: String =
    columns.underlying.keys
      .map(_.name)
      .mkString(start = "(", sep = ",", end = ")")

  private def renderInterpolatedValues: String =
    columns.underlying.keys
      .map(colName => s":${colName.name}")
      .mkString(start = "(", sep = ",", end = ")")

  private def renderIfNotExists: String =
    if (ifNotExists) "IF NOT EXISTS" else ""

  private def renderTimestampTTL: String =
    (timeToLive, timestamp) match {
      case (Some(ttl), Some(ts)) => s"USING TTL $ttl AND TIMESTAMP $ts"
      case (Some(ttl), None)     => s"USING TTL $ttl"
      case (None, Some(ts))      => s"USING TIMESTAMP $ts"
      case (None, None)          => ""
    }
}
object Insert {
  def into(table: String): Insert[InsertState.Empty] =
    new Insert(table, Columns.empty)
}

sealed trait InsertState
object InsertState {
  sealed trait Empty       extends InsertState
  sealed trait ColumnAdded extends InsertState
}
