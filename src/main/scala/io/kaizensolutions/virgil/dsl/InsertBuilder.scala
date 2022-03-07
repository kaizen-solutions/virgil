package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.internal.{BindMarker, BindMarkerName, BindMarkers}
import io.kaizensolutions.virgil.{CQL, MutationResult}

class InsertBuilder[State <: InsertState](
  private val table: String,
  private val columns: BindMarkers,
  private val timeToLive: Option[Int] = None,
  private val timestamp: Option[Long] = None,
  private val ifNotExists: Boolean = false
) {
  def value[ScalaType](columnName: String, inputValue: ScalaType)(implicit
    ev: CqlRowComponentEncoder[ScalaType]
  ): InsertBuilder[InsertState.ColumnAdded] = {
    val name   = BindMarkerName.make(columnName)
    val column = BindMarker.make(name, inputValue)(ev)
    new InsertBuilder(table, columns + column)
  }

  def ifNotExists(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new InsertBuilder(
      table = table,
      columns = columns,
      timeToLive = timeToLive,
      timestamp = timestamp,
      ifNotExists = true
    )
  }

  def usingTTL(timeToLive: Int)(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new InsertBuilder(table, columns, Some(timeToLive))
  }

  def usingTimestamp(
    timestamp: Long
  )(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    new InsertBuilder(table, columns, timeToLive, Some(timestamp))
  }

  def build(implicit stateEvidence: State <:< InsertState.ColumnAdded): CQL[MutationResult] = {
    // Make unused variables check happy
    val _ = stateEvidence
    CQL.insert(table, columns)
  }

  override def toString: String =
    s"""Insert(
       |  query = $renderInsertString,
       |  table = $table,
       |  columns = ${columns.toString},
       |)""".stripMargin

  private def renderInsertString: String =
    s"INSERT INTO $table $renderColumnNamesForInsertString VALUES $renderInterpolatedValuesForInsertString $renderIfNotExists $renderTimestampTTL"

  private def renderColumnNamesForInsertString: String =
    columns.underlying.keys
      .map(_.name)
      .mkString(start = "(", sep = ",", end = ")")

  private def renderInterpolatedValuesForInsertString: String =
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
object InsertBuilder {
  def apply(table: String): InsertBuilder[InsertState.Empty] =
    new InsertBuilder(table, BindMarkers.empty)
}

sealed trait InsertState
object InsertState {
  sealed trait Empty       extends InsertState
  sealed trait ColumnAdded extends InsertState
}
