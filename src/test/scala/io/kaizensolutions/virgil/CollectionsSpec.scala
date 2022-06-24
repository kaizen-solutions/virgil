package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.CollectionsSpecDatatypes._
import zio.test.TestAspect.samples
import zio.test._

object CollectionsSpec {
  def collectionsSpec: Spec[Sized with TestConfig with CQLExecutor, Any] =
    suite("Collections Specification") {
      test("Read and write a row containing collections") {
        import SimpleCollectionRow._
        check(simpleCollectionRowGen) { expected =>
          for {
            _         <- insert(expected).execute.runDrain
            result    <- select(expected.id).execute.runCollect
            resultAll <- selectAll.execute.runCollect
            actual     = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1) &&
            assertTrue(resultAll.contains(expected))
        }
      } + test("Persisting empty data into a collection will allow you to retrieve it") {
        import SimpleCollectionRow._
        val id        = 1000009
        val emptyData = SimpleCollectionRow(id = id, mapTest = Map.empty, setTest = Set.empty, listTest = Vector.empty)
        insert(emptyData).executeMutation *>
          select(id).execute.runHead.some.map(r =>
            assertTrue(r.id == id) && assertTrue(r.mapTest.isEmpty) && assertTrue(r.setTest.isEmpty) &&
              assertTrue(r.listTest.isEmpty)
          )
      } + test("Read and write a row containing nested collections") {
        import NestedCollectionRow._
        check(nestedCollectionRowGen) { expected =>
          for {
            _      <- insert(expected).execute.runDrain
            result <- select(expected.a).execute.runCollect
            actual  = result.head
          } yield assertTrue(actual == expected) && assertTrue(result.length == 1)
        }
      } + test("Read and write a row that contains an option of collections where the option is None") {
        check(optionCollectionRowGen) { popRow =>
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

  val simpleCollectionRowGen: Gen[Sized, SimpleCollectionRow] =
    for {
      id   <- Gen.int(1, 10000000)
      map  <- Gen.mapOf(key = Gen.int, value = Gen.string)
      set  <- Gen.setOf(Gen.long)
      list <- Gen.vectorOf(Gen.string)
    } yield SimpleCollectionRow(id, map, set, list)

  val nestedCollectionRowGen: Gen[Sized, NestedCollectionRow] =
    for {
      a <- Gen.int(1, 10000000)
      b <- Gen.mapOf(key = Gen.int, value = Gen.setOf(Gen.setOf(Gen.setOf(Gen.setOf(Gen.int)))))
    } yield NestedCollectionRow(a, b)

  val optionCollectionRowGen: Gen[Sized, OptionCollectionRow] =
    for {
      id   <- Gen.int(1, 10000000)
      map  <- Gen.option(Gen.mapOf(key = Gen.int, value = Gen.string))
      set  <- Gen.option(Gen.setOf(Gen.long))
      list <- Gen.option(Gen.vectorOf(Gen.string))
    } yield OptionCollectionRow(id, map, set, list)
}
