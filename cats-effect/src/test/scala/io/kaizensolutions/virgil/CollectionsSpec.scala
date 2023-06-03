package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes._
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

class CollectionsSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def maxParallelism: Int = 1

  override def checkConfig: CheckConfig =
    CheckConfig.default
      .copy(
        minimumSuccessful = 4,
        maximumGeneratorSize = 10,
        maximumDiscardRatio = 50,
        perPropertyParallelism = 1
      )

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Read and write a row containing collections") { executor =>
    import SimpleCollectionRow._
    forall(gen) { expected =>
      for {
        _         <- executor.executeMutation(insert(expected))
        actual    <- executor.execute(select(expected.id)).compile.toList
        resultAll <- executor.execute(selectAll).compile.toList
      } yield expect.all(actual.length == 1, actual.head == expected, resultAll.contains(expected))
    }
  }

  test("Persisting empty data into a collection will allow you to retrieve it") { executor =>
    import SimpleCollectionRow._
    val id        = 1000009
    val emptyData = SimpleCollectionRow(id = id, mapTest = Map.empty, setTest = Set.empty, listTest = Vector.empty)

    for {
      _      <- executor.executeMutation(insert(emptyData))
      result <- executor.execute(select(id)).compile.toList.map(_.headOption)
    } yield expect.all(result.isDefined, result.get.id == id)

  }

  test("Read and write a row containing nested collections") { executor =>
    import NestedCollectionRow._
    forall(gen) { expected =>
      for {
        _      <- executor.executeMutation(insert(expected))
        result <- executor.execute(select(expected.a)).compile.toList
        actual  = result.head
      } yield expect.all(actual == expected, result.length == 1)
    }
  }

  test("Read and write a row that contains an option of collections where the option is None") { executor =>
    forall(OptionCollectionRow.gen) { popRow =>
      for {
        _            <- executor.executeMutation(OptionCollectionRow.truncate)
        _            <- executor.executeMutation(OptionCollectionRow.insert(OptionCollectionRow(1, None, None, None)))
        dbResults    <- executor.execute(OptionCollectionRow.select(1)).compile.toList
        result        = dbResults.head
        _            <- executor.executeMutation(OptionCollectionRow.insert(popRow))
        dbResultsPop <- executor.execute(OptionCollectionRow.select(popRow.id)).compile.toList
        resultPop     = dbResultsPop.head
      } yield expect.all(result == OptionCollectionRow(1, None, None, None), resultPop == popRow)
    }
  }
}
