package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.codecs._
import zio.Has
import zio.random.Random
import zio.test.TestAspect.samples
import zio.test._

object CollectionsSpec {
  def collectionsSpec: ZSpec[Has[CQLExecutor] with Random with Sized with TestConfig, Throwable] =
    suite("Collections Specification") {
      testM("Read and write a row containing collections") {
        import SimpleCollectionRow._
        checkM(gen) { expected =>
          for {
            _      <- CQLExecutor.execute(insert(expected)).runDrain
            result <- CQLExecutor.execute(select(expected.id)).runCollect
            actual  = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1)
        }
      } + testM("Read and write a row containing nested collections") {
        import NestedCollectionRow._
        checkM(gen) { expected =>
          for {
            _      <- CQLExecutor.execute(insert(expected)).runDrain
            result <- CQLExecutor.execute(select(expected.a)).runCollect
            actual  = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1)
        }
      }
    } @@ samples(10)
}

final case class SimpleCollectionRow(
  id: Int,
  mapTest: Map[Int, String],
  setTest: Set[Long],
  listTest: List[String]
)
object SimpleCollectionRow {
  implicit val readerForSimpleCollectionRow: Reader[SimpleCollectionRow] = Reader.derive[SimpleCollectionRow]
  implicit val writerForSimpleCollectionRow: Writer[SimpleCollectionRow] = Writer.derive[SimpleCollectionRow]

  def insert(in: SimpleCollectionRow): CQL[MutationResult] =
    InsertBuilder("collectionspec_simplecollectiontable")
      .value("id", in.id)
      .value("mapTest", in.mapTest)
      .value("setTest", in.setTest)
      .value("listTest", in.listTest)
      .build

  def select(id: Int): CQL[SimpleCollectionRow] =
    SelectBuilder
      .from("collectionspec_simplecollectiontable")
      .column("id")
      .column("mapTest")
      .column("setTest")
      .column("listTest")
      .where("id" === id)
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
  implicit val readerForNestedCollectionRow: Reader[NestedCollectionRow] = Reader.derive[NestedCollectionRow]
  implicit val writerForNestedCollectionRow: Writer[NestedCollectionRow] = Writer.derive[NestedCollectionRow]

  def select(a: Int) =
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
