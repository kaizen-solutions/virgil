package io.kaizensolutions.virgil.internal

import io.kaizensolutions.virgil.dsl.Relation
import zio.{Chunk, NonEmptyChunk}

sealed private[virgil] trait QueryType
object QueryType {
  final private[virgil] case class Select(
    tableName: String,
    columnNames: NonEmptyChunk[String],
    relations: Chunk[Relation]
  ) extends QueryType

  final private[virgil] case class RawCql(
    query: String,
    columns: BindMarkers
  ) extends QueryType
}
