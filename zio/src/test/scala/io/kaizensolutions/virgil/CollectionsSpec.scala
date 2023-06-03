package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes._
import zio.test.TestAspect.samples
import zio.test._
import zio.test.scalacheck._

object CollectionsSpec {
  def collectionsSpec =
    suite("Collections Specification")(
      test("Read and write a row containing collections") {
        import SimpleCollectionRow._
        check(gen.toGenZIO) { expected =>
          for {
            _         <- insert(expected).execute.runDrain
            result    <- select(expected.id).execute.runCollect
            resultAll <- selectAll.execute.runCollect
            actual     = result.head
          } yield assertTrue(actual == expected, result.length == 1, resultAll.contains(expected))
        }
      },
      test("Persisting empty data into a collection will allow you to retrieve it") {
        import SimpleCollectionRow._
        val id        = 1000009
        val emptyData = SimpleCollectionRow(id = id, mapTest = Map.empty, setTest = Set.empty, listTest = Vector.empty)
        insert(emptyData).executeMutation *>
          select(id).execute.runHead.some.map(r =>
            assertTrue(r.id == id, r.mapTest.isEmpty, r.setTest.isEmpty, r.listTest.isEmpty)
          )
      },
      test("Read and write a row containing nested collections") {
        import NestedCollectionRow._
        // NOTE: I think there is something with the eagerness of the ScalaCheck generator that causes an out of memory exception
        check(zioGen) { expected =>
          for {
            _      <- insert(expected).execute.runDrain
            result <- select(expected.a).execute.runCollect
            actual  = result.head
          } yield assertTrue(actual == expected, result.length == 1)
        }
      },
      test("Read and write a row that contains an option of collections where the option is None") {
        check(OptionCollectionRow.gen.toGenZIO) { popRow =>
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
          } yield assertTrue(result == OptionCollectionRow(1, None, None, None), resultPop == popRow)
        }
      }
    ) @@ samples(4)
}
