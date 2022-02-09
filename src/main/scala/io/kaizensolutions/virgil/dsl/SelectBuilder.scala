package io.kaizensolutions.virgil.dsl

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.{BindMarkerName, Query, QueryType}
import zio.{Chunk, NonEmptyChunk}

class SelectBuilder[State <: SelectState](
  private val tableName: String,
  private val columnNames: Chunk[String],
  private val relations: Chunk[Relation]
) {
  def column(name: String)(implicit ev: State <:< SelectState.Empty): SelectBuilder[SelectState.NonEmpty] = {
    val _ = ev
    new SelectBuilder(tableName, columnNames :+ name, relations)
  }

  def columns(name: String, names: String*)(implicit
    ev: State <:< SelectState.Empty
  ): SelectBuilder[SelectState.NonEmpty] = {
    val _        = ev
    val allNames = Chunk.fromIterable(name +: names)
    new SelectBuilder(tableName, columnNames ++ allNames, relations)
  }

  def where(relation: Relation)(implicit ev: State =:= SelectState.NonEmpty): SelectBuilder[SelectState.Relation] = {
    val _ = ev
    new SelectBuilder(tableName, columnNames, relations :+ relation)
  }

  def and(relation: Relation)(implicit ev: State =:= SelectState.Relation): SelectBuilder[SelectState.Relation] = {
    val _ = ev
    new SelectBuilder(tableName, columnNames, relations :+ relation)
  }

  def buildRow(implicit ev: State <:< SelectState.NonEmpty): Query[Row] = {
    val _       = ev
    val columns = NonEmptyChunk.fromChunk(columnNames).get.map(BindMarkerName.make)
    Query(
      queryType = QueryType.Select(tableName, columns, relations),
      reader = Reader.cassandraRowReader
    )
  }

  def build[FromCassandra <: Product](implicit
    readerEv: Reader[FromCassandra],
    stateEv: State <:< SelectState.NonEmpty
  ): Query[FromCassandra] =
    buildRow(stateEv).withOutput[FromCassandra](readerEv)
}
object SelectBuilder {
  def from(tableName: String): SelectBuilder[SelectState.Empty] =
    new SelectBuilder[SelectState.Empty](tableName, Chunk.empty, Chunk.empty)
}

sealed trait SelectState
object SelectState {
  sealed trait Empty    extends SelectState
  sealed trait NonEmpty extends Empty
  sealed trait Relation extends NonEmpty
}
