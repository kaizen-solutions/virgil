package io.kaizensolutions.virgil.internal

import io.kaizensolutions.virgil.dsl.Relation

sealed private[virgil] trait QueryType
object QueryType {
  final private[virgil] case class Select(
    tableName: String,
    columnNames: IndexedSeq[String],
    relations: IndexedSeq[Relation]
  ) extends QueryType

  final private[virgil] case class RawCql(
    query: String,
    columns: BindMarkers
  ) extends QueryType
}
