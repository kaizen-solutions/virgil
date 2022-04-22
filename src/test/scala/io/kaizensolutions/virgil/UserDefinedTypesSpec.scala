package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.UserDefinedTypesSpecDatatypes._
import zio.Has
import zio.duration._
import zio.random.Random
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live

import java.time.{LocalDate, LocalTime}

object UserDefinedTypesSpec {
  def userDefinedTypesSpec: ZSpec[Live with Has[CQLExecutor] with Random with Sized with TestConfig, Throwable] =
    suite("User Defined Types specification") {
      testM("Write and read Person rows containing UDTs which are nested") {
        import Row_Person._
        checkM(row_PersonGen) { expected =>
          val insertPerson = insert(expected).execute.runDrain
          val fetchActual  = select(expected.id).execute.runCollect

          for {
            _              <- insertPerson
            actualWithData <- fetchActual
          } yield assertTrue(actualWithData.head == expected) && assertTrue(actualWithData.length == 1)
        }
      } +
        testM(
          "Write and read rows for a UDT containing nested UDTs within themselves along with nested collections containing UDTs"
        ) {
          import Row_HeavilyNestedUDTTable._
          checkM(row_HeavilyNestedUDTTableGen) { expected =>
            val insertPeople = insert(expected).execute.runDrain
            val fetchActual  = select(expected.id).execute.runCollect

            for {
              _      <- insertPeople
              actual <- fetchActual
            } yield assertTrue(actual.head == expected) && assertTrue(actual.length == 1)
          }
        }
    } @@ timeout(1.minute) @@ samples(10)

  def row_PersonGen: Gen[Random, Row_Person] =
    for {
      id   <- Gen.int(1, 100000)
      name <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
      age  <- Gen.int(18, 100)
      data <- uDT_DataGen
    } yield Row_Person(id, name, age, data)

  def row_HeavilyNestedUDTTableGen: Gen[Random with Sized, Row_HeavilyNestedUDTTable] =
    for {
      id   <- Gen.anyInt
      data <- uDT_ExampleCollectionNestedUDTTypeGen
    } yield Row_HeavilyNestedUDTTable(id, data)

  def uDT_DataGen: Gen[Random, UDT_Data] =
    for {
      addresses <- Gen.listOfBounded(10, 20)(uDT_AddressGen)
      email     <- Gen.option(uDT_EmailGen)
    } yield UDT_Data(addresses, email)

  def uDT_AddressGen: Gen[Random, UDT_Address] =
    for {
      number <- Gen.int(1, 10000)
      street <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
      city   <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
    } yield UDT_Address(number, street, city)

  def uDT_EmailGen: Gen[Random, UDT_Email] =
    for {
      username   <- Gen.stringBounded(3, 10)(Gen.alphaNumericChar)
      domainName <- Gen.stringBounded(4, 32)(Gen.alphaNumericChar)
      domain     <- Gen.oneOf(Gen.const("com"), Gen.const("org"), Gen.const("net"))
    } yield UDT_Email(username, domainName, domain)

  def uDT_ExampleTypeGen: Gen[Random, UDT_ExampleType] =
    for {
      x <- Gen.anyLong
      y <- Gen.anyLong
      // Interesting note: the Java date and time library can express a range of dates and times far greater than what Cassandra supports
      day    <- Gen.int(1, 28)
      month  <- Gen.int(1, 12)
      year   <- Gen.int(1999, 2050)
      hour   <- Gen.int(0, 23)
      minute <- Gen.int(0, 59)
      date   <- Gen.const(LocalDate.of(year, month, day))
      time   <- Gen.oneOf(Gen.const(LocalTime.of(hour, minute)))
    } yield UDT_ExampleType(
      x = x,
      y = y,
      date = date,
      time = time
    )

  def uDT_ExampleNestedTypeGen: Gen[Random with Sized, UDT_ExampleNestedType] =
    for {
      a <- Gen.anyInt
      b <- Gen.alphaNumericStringBounded(4, 10)
      c <- uDT_ExampleTypeGen
    } yield UDT_ExampleNestedType(a, b, c)

  def uDT_ExampleCollectionNestedUDTTypeGen: Gen[Random with Sized, UDT_ExampleCollectionNestedUDTType] =
    for {
      a <- Gen.anyInt
      b <- Gen.mapOf(
             key = Gen.anyInt,
             value = Gen.setOf(
               Gen.setOf(
                 Gen.setOf(
                   Gen.setOf(uDT_ExampleNestedTypeGen)
                 )
               )
             )
           )
      c <- uDT_ExampleNestedTypeGen
    } yield UDT_ExampleCollectionNestedUDTType(a, b, c)
}
