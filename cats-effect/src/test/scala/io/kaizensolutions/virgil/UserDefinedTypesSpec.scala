package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes._
import weaver._
import weaver.scalacheck.Checkers

class UserDefinedTypesSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Write and read Person rows containing UDTs which are nested") { executor =>
    import Row_Person._

    forall(gen) { expected =>
      for {
        _      <- executor.executeMutation(insert(expected))
        actual <- executor.execute(select(expected.id)).compile.toList
      } yield expect.all(actual.head == expected, actual.length == 1)
    }
  }

  test(
    "Write and read rows for a UDT containing nested UDTs within themselves along with nested collections containing UDTs"
  ) { executor =>
    import Row_HeavilyNestedUDTTable._

    for {
      expected <- IO.delay(sample)
      _        <- executor.executeMutation(insert(expected))
      actual   <- executor.execute(select(expected.id)).compile.toList
    } yield expect.all(actual.head == expected, actual.length == 1)

  }
}
