package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.{CQL, MutationResult}

object CollectionsSpecDatatypes {
  final case class SimpleCollectionRow(
    id: Int,
    @CqlColumn("map_test") mapTest: Map[Int, String],
    @CqlColumn("set_test") setTest: Set[Long],
    @CqlColumn("list_test") listTest: Vector[String]
  )
  object SimpleCollectionRow extends SimpleCollectionRowInstances {

    def insert(in: SimpleCollectionRow): CQL[MutationResult] =
      InsertBuilder("collectionspec_simplecollectiontable")
        .value("id", in.id)
        .value("map_test", in.mapTest)
        .value("set_test", in.setTest)
        .value("list_test", in.listTest)
        .build

    def select(id: Int): CQL[SimpleCollectionRow] =
      SelectBuilder
        .from("collectionspec_simplecollectiontable")
        .column("id")
        .column("map_test")
        .column("set_test")
        .column("list_test")
        .where("id" === id)
        .build[SimpleCollectionRow]

    val selectAll: CQL[SimpleCollectionRow] =
      SelectBuilder
        .from("collectionspec_simplecollectiontable")
        .columns("id", "map_test", "set_test", "list_test")
        .build[SimpleCollectionRow]
  }

  final case class NestedCollectionRow(
    a: Int,
    b: Map[Int, Set[Set[Set[Set[Int]]]]]
  )
  object NestedCollectionRow extends NestedCollectionRowInstances {

    def select(a: Int): CQL[NestedCollectionRow] =
      SelectBuilder
        .from("collectionspec_nestedcollectiontable")
        .column("a")
        .column("b")
        .where("a" === a)
        .build[NestedCollectionRow]

    def insert(in: NestedCollectionRow): CQL[MutationResult] =
      InsertBuilder("collectionspec_nestedcollectiontable")
        .value("a", in.a)
        .value("b", in.b)
        .build
  }

  final case class OptionCollectionRow(
    id: Int,
    @CqlColumn("opt_map_test") m: Option[Map[Int, String]],
    @CqlColumn("opt_set_test") s: Option[Set[Long]],
    @CqlColumn("opt_list_test") l: Option[Vector[String]]
  )
  object OptionCollectionRow extends OptionCollectionRowInstances {
    private val tableName = "collectionspec_optioncollectiontable"
    def truncate: CQL[MutationResult] =
      (cql"TRUNCATE " ++ tableName.asCql).mutation

    def insert(in: OptionCollectionRow): CQL[MutationResult] =
      InsertBuilder(tableName)
        .value("id", in.id)
        .value("opt_map_test", in.m)
        .value("opt_set_test", in.s)
        .value("opt_list_test", in.l)
        .build

    def select(id: Int): CQL[OptionCollectionRow] =
      SelectBuilder
        .from(tableName)
        .column("id")
        .column("opt_map_test")
        .column("opt_set_test")
        .column("opt_list_test")
        .where("id" === id)
        .build[OptionCollectionRow]

    val selectAll: CQL[OptionCollectionRow] =
      SelectBuilder
        .from(tableName)
        .columns("id", "opt_map_test", "opt_set_test", "opt_list_test")
        .build[OptionCollectionRow]
  }
}
