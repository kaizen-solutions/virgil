package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes._
import zio.test._
import zio.{test => _, _}

object UserDefinedTypesSpec {
  def userDefinedTypesSpec: Spec[Sized & CQLExecutor, Throwable] =
    suite("User Defined Types specification") {
      test("Write and read Person rows containing UDTs which are nested") {
        import Row_Person._
        check(zioGen) { expected =>
          val insertPerson = insert(expected).execute.runDrain
          val fetchActual  = select(expected.id).execute.runCollect

          for {
            _              <- insertPerson
            actualWithData <- fetchActual
          } yield assertTrue(actualWithData.head == expected, actualWithData.length == 1)
        }
      } +
        test(
          "Write and read rows for a UDT containing nested UDTs within themselves along with nested collections containing UDTs"
        ) {
          import Row_HeavilyNestedUDTTable._
          check(zioGen) { expected =>
            val insertPeople = insert(expected).execute.runDrain
            val fetchActual  = select(expected.id).execute.runCollect

            for {
              _      <- insertPeople
              actual <- fetchActual
            } yield assertTrue(actual.head == expected, actual.length == 1)
          }
        }
    } @@ TestAspect.timeout(1.minute) @@ TestAspect.samples(10)
}
