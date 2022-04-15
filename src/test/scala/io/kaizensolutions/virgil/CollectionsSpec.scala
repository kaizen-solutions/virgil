package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import zio.random.Random
import zio.test.TestAspect.samples
import zio.test._

object CollectionsSpec {
  def collectionsSpec =
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
      } + testM("Persisting empty data into a collection will allow you to retrieve it") {
        import SimpleCollectionRow._
        val id        = 1000009
        val emptyData = SimpleCollectionRow(id = id, mapTest = Map.empty, setTest = Set.empty, listTest = Vector.empty)
        insert(emptyData).executeMutation *>
          select(id).execute.runHead.some.map(r =>
            assertTrue(r.id == id) && assertTrue(r.mapTest.isEmpty) && assertTrue(r.setTest.isEmpty) &&
              assertTrue(r.listTest.isEmpty)
          )
      } + testM("Read and write a row containing nested collections") {
        import NestedCollectionRow._
        checkM(gen) { expected =>
          for {
            _      <- insert(expected).execute.runDrain
            result <- select(expected.a).execute.runCollect
            actual  = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1)
        }
      } + testM("Read and write a row that contains an option of collections where the option is None") {
        checkM(OptionCollectionRow.gen) { popRow =>
          for {
            // Please note that Cassandra does not have the concept of nullable collection data.
            // So if you persist an empty collection wrapped in an Option, you'll get back None
            _            <- OptionCollectionRow.truncate.execute.runDrain
            _            <- OptionCollectionRow.insert(OptionCollectionRow(1, None, None, None)).execute.runDrain
            dbResults    <- OptionCollectionRow.select(1).execute.runCollect
            result        = dbResults.head
            _            <- OptionCollectionRow.insert(popRow).execute.runDrain
            dbResultsPop <- OptionCollectionRow.select(popRow.id).execute.runCollect
            resultPop     = dbResultsPop.head
          } yield assertTrue(result == OptionCollectionRow(1, None, None, None)) && assertTrue(resultPop == popRow)
        }
      }
    } @@ samples(10)
}

final case class SimpleCollectionRow(
  id: Int,
  @CqlColumn("map_test") mapTest: Map[Int, String],
  @CqlColumn("set_test") setTest: Set[Long],
  @CqlColumn("list_test") listTest: Vector[String]
)
object SimpleCollectionRow {

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
      list <- Gen.vectorOf(Gen.anyString)
    } yield SimpleCollectionRow(id, map, set, list)
}

final case class NestedCollectionRow(
  a: Int,
  b: Map[Int, Set[Set[Set[Set[Int]]]]]
)
object NestedCollectionRow {

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

final case class OptionCollectionRow(
  id: Int,
  @CqlColumn("opt_map_test") m: Option[Map[Int, String]],
  @CqlColumn("opt_set_test") s: Option[Set[Long]],
  @CqlColumn("opt_list_test") l: Option[Vector[String]]
)
object OptionCollectionRow {
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

  def gen: Gen[Random with Sized, OptionCollectionRow] =
    for {
      id   <- Gen.int(1, 10000000)
      map  <- Gen.option(Gen.mapOf(key = Gen.anyInt, value = Gen.anyString))
      set  <- Gen.option(Gen.setOf(Gen.anyLong))
      list <- Gen.option(Gen.vectorOf(Gen.anyString))
    } yield OptionCollectionRow(id, map, set, list)
}
