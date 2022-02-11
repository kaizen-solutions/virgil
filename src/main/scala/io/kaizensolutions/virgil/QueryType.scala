package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl.Relation
import zio.{Chunk, NonEmptyChunk}

sealed trait QueryType
object QueryType {
  final case class Select[FromCassandra](
    tableName: String,
    columnNames: NonEmptyChunk[String],
    relations: Chunk[Relation]
  ) extends QueryType

  final private[virgil] case class RawCql[FromCassandra] private (
    query: String,
    columns: BindMarkers
  ) extends QueryType
}
