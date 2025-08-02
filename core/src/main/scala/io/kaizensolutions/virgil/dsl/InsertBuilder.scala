package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.cql.ValueInCql
import io.kaizensolutions.virgil.internal.BindMarkers

import java.time.Duration
import scala.collection.immutable.ListMap

final case class InsertBuilder[State <: InsertState](
  private val table: String,
  private val columns: BindMarkers,
  private val timeToLive: Option[Duration] = None,
  private val timestamp: Option[Long] = None,
  private val conditions: InsertConditions = Conditions.NoConditions
) {
  def value[ScalaType](columnName: String, inputValue: ScalaType)(implicit
    ev: CqlRowComponentEncoder[ScalaType]
  ): InsertBuilder[InsertState.ColumnAdded] =
    values(columnName -> inputValue)

  def values(column: (String, ValueInCql), rest: (String, ValueInCql)*): InsertBuilder[InsertState.ColumnAdded] = {
    val allColumns                              = column +: rest
    val underlying: ListMap[String, ValueInCql] = {
      val acc         = ListMap.newBuilder[String, ValueInCql]
      val withColumns = acc ++= allColumns
      withColumns.result()
    }
    val columnsToAdd = BindMarkers.from(underlying)

    copy(columns = columns ++ columnsToAdd)
  }

  def ifNotExists(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    copy(conditions = Conditions.IfNotExists)
  }

  def usingTTL(
    timeToLive: Duration
  )(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    copy(timeToLive = Option(timeToLive))
  }

  def usingTimestamp(
    timestamp: Long
  )(implicit stateEvidence: State <:< InsertState.ColumnAdded): InsertBuilder[State] = {
    // Make unused variables check happy
    val _ = stateEvidence
    copy(timestamp = Option(timestamp))
  }

  def build(implicit stateEvidence: State <:< InsertState.ColumnAdded): CQL[MutationResult] = {
    // Make unused variables check happy
    val _ = stateEvidence
    CQL.insert(table, columns, conditions, timeToLive, timestamp)
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
