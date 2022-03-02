package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.codecs._
import io.kaizensolutions.virgil.dsl._
import zio.Has
import zio.random.Random
import zio.schema.{DeriveSchema, Schema}
import zio.test.TestAspect.samples
import zio.test._

object CollectionsSpec {
  def collectionsSpec: ZSpec[Has[CQLExecutor] with Random with Sized with TestConfig, Throwable] =
    suite("Collections Specification") {
      testM("Read and write a row containing collections") {
        import SimpleCollectionRow._
        checkM(gen) { expected =>
          for {
            _         <- insert(expected).execute.runDrain
            result    <- select(expected.id).execute.runCollect
            resultAll <- selectAll.execute.runCollect
            actual     = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1) &&
            assertTrue(resultAll.contains(expected))
        }
      } + testM("Read and write a row containing nested collections") {
        import NestedCollectionRow._
        checkM(gen) { expected =>
          for {
            _      <- insert(expected).execute.runDrain
            result <- select(expected.a).execute.runCollect
            actual  = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1)
        }
      }
    } @@ samples(10)
}

final case class SimpleCollectionRow(
  id: Int,
  @CqlColumn("map_test") mapTest: Map[Int, String],
  @CqlColumn("set_test") setTest: Set[Long],
  @CqlColumn("list_test") listTest: List[String]
)
object SimpleCollectionRow {
  implicit val schemaForSimpleCollectionRow: Schema[SimpleCollectionRow] = DeriveSchema.gen[SimpleCollectionRow]
  implicit val cqlDecoderForSimpleCollectionRow: CqlDecoder[SimpleCollectionRow] =
    CqlDecoder.derive[SimpleCollectionRow]

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

  def gen: Gen[Random with Sized, SimpleCollectionRow] =
    for {
      id   <- Gen.int(1, 10000000)
      map  <- Gen.mapOf(key = Gen.anyInt, value = Gen.anyString)
      set  <- Gen.setOf(Gen.anyLong)
      list <- Gen.listOf(Gen.anyString)
    } yield SimpleCollectionRow(id, map, set, list)
}

final case class NestedCollectionRow(
  a: Int,
  b: Map[Int, Set[Set[Set[Set[Int]]]]]
)
object NestedCollectionRow {
  implicit val schemaForNestedCollectionRow: Schema[NestedCollectionRow] =
    DeriveSchema.gen[NestedCollectionRow]

  implicit val cqlDecoderForNestedCollectionRow: CqlDecoder[NestedCollectionRow] =
    CqlDecoder.derive[NestedCollectionRow]

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

  def gen: Gen[Random with Sized, NestedCollectionRow] =
    for {
      a <- Gen.int(1, 10000000)
      b <- Gen.mapOf(key = Gen.anyInt, value = Gen.setOf(Gen.setOf(Gen.setOf(Gen.setOf(Gen.anyInt)))))
    } yield NestedCollectionRow(a, b)
}
