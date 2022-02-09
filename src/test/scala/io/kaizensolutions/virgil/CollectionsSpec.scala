package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.codecs._
import zio.Has
import zio.random.Random
import zio.test.TestAspect.samples
import zio.test._

object CollectionsSpec {
  def collectionsSpec: ZSpec[Has[ZioCassandraSession] with Random with Sized with TestConfig, Throwable] =
    suite("Collections Specification") {
      testM("Read and write a row containing collections") {
        import SimpleCollectionRow._
        checkM(gen) { expected =>
          for {
            _      <- ZioCassandraSession.execute(insert(expected))
            result <- ZioCassandraSession.select(select(expected.id)).runCollect
            actual  = result.head
          } yield assertTrue(actual == expected)
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

  def insert(in: SimpleCollectionRow): Mutation =
    Mutation.Insert
      .into("collectionspec_simplecollectiontable")
      .value("id", in.id)
      .value("mapTest", in.mapTest)
      .value("setTest", in.setTest)
      .value("listTest", in.listTest)
      .build

  def select(id: Int) =
    Query.select
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
