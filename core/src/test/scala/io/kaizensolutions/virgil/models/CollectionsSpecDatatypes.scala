package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen
import zio.test.{Gen => ZGen}

object CollectionsSpecDatatypes {
  final case class SimpleCollectionRow(
    id: Int,
    @CqlColumn("map_test") mapTest: Map[Int, String],
    @CqlColumn("set_test") setTest: Set[Long],
    @CqlColumn("list_test") listTest: Vector[String]
  )
  object SimpleCollectionRow extends SimpleCollectionRowInstances {
    val gen: Gen[SimpleCollectionRow] =
      for {
        id   <- Gen.uuid.map(_.hashCode().abs.toInt)
        map  <- Gen.map(key = Gen.posNum[Int], value = Gen.stringOf(Gen.alphaNumChar))
        set  <- Gen.nonEmptySetOf(Gen.long)
        list <- Gen.nonEmptyVectorOf(Gen.stringOf(Gen.alphaNumChar))
      } yield SimpleCollectionRow(id, map, set, list)

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
    val zioGen: ZGen[Any, NestedCollectionRow] =
      for {
        a <- ZGen.int(1, 10000000)
        b <- ZGen.mapOf(key = ZGen.int, value = ZGen.setOf(ZGen.setOf(ZGen.setOf(ZGen.setOf(ZGen.int)))))
      } yield NestedCollectionRow(a, b)

    val gen: Gen[NestedCollectionRow] =
      for {
        a <- Gen.int
        b <-
          Gen.lzy(
            Gen.map(
              key = Gen.int,
              value = Gen.lzy(Gen.setOf(Gen.lzy(Gen.setOf(Gen.lzy(Gen.setOf(Gen.lzy(Gen.setOf(Gen.lzy(Gen.int)))))))))
            )
          )
      } yield NestedCollectionRow(a, b)

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
    val gen: Gen[OptionCollectionRow] =
      for {
        id   <- Gen.posNum[Int]
        map  <- Gen.option(Gen.map(key = Gen.int, value = Gen.stringOf(Gen.alphaNumChar)))
        set  <- Gen.option(Gen.setOf(Gen.long))
        list <- Gen.option(Gen.vectorOf(Gen.stringOf(Gen.alphaNumChar)))
      } yield OptionCollectionRow(id, map, set, list)

    private val tableName             = "collectionspec_optioncollectiontable"
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
