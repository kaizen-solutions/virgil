package io.kaizensolutions.virgil.dsl

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.codecs.CqlRowDecoder

final case class SelectBuilder[State <: SelectState](
  private val tableName: String,
  private val columnNames: IndexedSeq[String],
  private val relations: IndexedSeq[Relation]
) {
  def allColumns(implicit ev: State =:= SelectState.Empty): SelectBuilder[SelectState.NonEmpty] = {
    val _ = ev
    copy(columnNames = IndexedSeq("*"))
  }

  def column(name: String)(implicit ev: State <:< SelectState.Empty): SelectBuilder[SelectState.NonEmpty] = {
    val _ = ev
    copy(columnNames = columnNames :+ name)
  }

  def columns(name: String, names: String*)(implicit
    ev: State <:< SelectState.Empty
  ): SelectBuilder[SelectState.NonEmpty] = {
    val _        = ev
    val allNames = IndexedSeq.concat(name +: names)
    copy(columnNames = columnNames ++ allNames)
  }

  def columns(names: IndexedSeq[String])(implicit
    ev: State <:< SelectState.Empty
  ): SelectBuilder[SelectState.NonEmpty] = {
    val _ = ev
    copy(columnNames = columnNames ++ names)
  }

  def where(relation: Relation)(implicit ev: State =:= SelectState.NonEmpty): SelectBuilder[SelectState.Relation] = {
    val _ = ev
    copy(relations = relations :+ relation)
  }

  def and(relation: Relation)(implicit ev: State =:= SelectState.Relation): SelectBuilder[SelectState.Relation] = {
    val _ = ev
    copy(relations = relations :+ relation)
  }

  def buildRow(implicit ev: State <:< SelectState.NonEmpty): CQL[Row] = {
    val _ = ev

    CQL.select[Row](
      tableName = tableName,
      columns = columnNames,
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
    new SelectBuilder[SelectState.Empty](tableName, IndexedSeq.empty, IndexedSeq.empty)
}

sealed trait SelectState
object SelectState {
  sealed trait Empty    extends SelectState
  sealed trait NonEmpty extends Empty
  sealed trait Relation extends NonEmpty
}
