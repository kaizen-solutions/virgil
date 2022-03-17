package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.internal.{BindMarker, BindMarkerName, BindMarkers}
import io.kaizensolutions.virgil.{CQL, MutationResult}

class InsertBuilder[State <: InsertState](
  private val table: String,
  private val columns: BindMarkers,
  private val timeToLive: Option[Int] = None,
  private val timestamp: Option[Long] = None,
  private val conditions: InsertConditions = Conditions.NoConditions
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
      conditions = Conditions.IfNotExists
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
    CQL.insert(table, columns, conditions)
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
