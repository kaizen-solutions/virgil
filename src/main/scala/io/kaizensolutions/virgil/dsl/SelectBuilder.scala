package io.kaizensolutions.virgil.dsl

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.codecs.CqlRowDecoder
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

  def buildRow(implicit ev: State <:< SelectState.NonEmpty): CQL[Row] = {
    val _       = ev
    val columns = NonEmptyChunk.fromChunk(columnNames).get
    CQL.select[Row](
      tableName = tableName,
      columns = columns,
      relations = relations
    )
  }

  def build[FromCassandra <: Product](implicit
    decoderEv: CqlRowDecoder.Object[FromCassandra],
    stateEv: State <:< SelectState.NonEmpty
  ): CQL[FromCassandra] =
    buildRow(stateEv).readAs[FromCassandra]
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
