package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.dsl.{Relation, SelectBuilder}
import zio.{Chunk, NonEmptyChunk}

/**
 * Queries map 1:1 with CQL SELECT statements and return a list of results which
 * can be viewed as any datatype having the Reader capability
 *
 * @param queryType
 *   is the type of Query (Select which is the higher level DSL and RawCql which
 *   is the lower level DSL)
 * @param reader
 *   is the capability to read data from Cassandra
 * @tparam FromCassandra
 *   is the type of data we expect to read from Cassandra
 */
final case class Query[FromCassandra](queryType: QueryType, reader: Reader[FromCassandra]) {
  def withOutput[FromCassandra2](implicit reader: Reader[FromCassandra2]): Query[FromCassandra2] =
    Query(queryType, reader)
}
object Query {
  def select: SelectBuilder.type = SelectBuilder
}

sealed trait QueryType
object QueryType {
  final case class Select[FromCassandra](
    tableName: String,
    columnNames: NonEmptyChunk[BindMarkerName],
    relations: Chunk[Relation]
  ) extends QueryType

  final private[virgil] case class RawCql[FromCassandra] private (
    query: String,
    columns: BindMarkers
  ) extends QueryType
}
