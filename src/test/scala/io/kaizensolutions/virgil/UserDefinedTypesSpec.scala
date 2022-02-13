package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.codecs.{Reader, Writer}
import io.kaizensolutions.virgil.codecs.userdefinedtypes.{UdtReader, UdtWriter}
import io.kaizensolutions.virgil.cql._
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
        checkM(Row_Person.gen) { expected =>
          val insertPeople = insert(expected).execute.runDrain
          val fetchActual  = select(expected.id).execute.runCollect

          for {
            _      <- insertPeople
            actual <- fetchActual
          } yield assertTrue(actual.head == expected) && assertTrue(actual.length == 1)
        }
      } + testM(
        "Write and read rows for a UDT containing nested UDTs within themselves along with nested collections containing UDTs"
      ) {
        import Row_HeavilyNestedUDTTable._
        checkM(gen) { expected =>
          val insertPeople = insert(expected).execute.runDrain
          val fetchActual  = select(expected.id).execute.runCollect

          for {
            _      <- insertPeople
            actual <- fetchActual
          } yield assertTrue(actual.head == expected) && assertTrue(actual.length == 1)
        }
      }
    } @@ timeout(1.minute) @@ samples(10)
}

final case class Row_Person(
  id: Int,
  name: String,
  age: Int,
  data: UDT_Data
)
object Row_Person {
  implicit val readerForRowPerson: Reader[Row_Person] = Reader.derive[Row_Person]

  def insert(person: Row_Person): CQL[MutationResult] =
    cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".mutation

  def select(id: Int): CQL[Row_Person] =
    cql"SELECT id, name, age, data FROM userdefinedtypesspec_person WHERE id = $id".query[Row_Person]

  def gen: Gen[Random, Row_Person] =
    for {
      id   <- Gen.int(1, 100000)
      name <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
      age  <- Gen.int(18, 100)
      data <- UDT_Data.gen
    } yield Row_Person(id, name, age, data)
}

final case class UDT_Data(
  addresses: List[UDT_Address],
  email: Option[UDT_Email]
)
object UDT_Data {
  implicit val readerForUdtData: Reader[UDT_Data] = UdtReader.deriveReader[UDT_Data]
  def gen: Gen[Random, UDT_Data] =
    for {
      addresses <- Gen.listOfBounded(10, 20)(UDT_Address.gen)
      email     <- Gen.option(UDT_Email.gen)
    } yield UDT_Data(addresses, email)
}

final case class UDT_Address(
  number: Int,
  street: String,
  city: String
)
object UDT_Address {
  implicit val readerForUDTAddress: Reader[UDT_Address] = UdtReader.deriveReader[UDT_Address]

  def gen: Gen[Random, UDT_Address] =
    for {
      number <- Gen.int(1, 10000)
      street <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
      city   <- Gen.stringBounded(4, 10)(Gen.alphaNumericChar)
    } yield UDT_Address(number, street, city)
}

final case class UDT_Email(
  username: String,
  domain_name: String,
  domain: String
)
object UDT_Email {
  implicit val readerForUDTEmail: Reader[UDT_Email] = UdtReader.deriveReader[UDT_Email]

  def gen: Gen[Random, UDT_Email] =
    for {
      username   <- Gen.stringBounded(3, 10)(Gen.alphaNumericChar)
      domainName <- Gen.stringBounded(4, 32)(Gen.alphaNumericChar)
      domain     <- Gen.oneOf(Gen.const("com"), Gen.const("org"), Gen.const("net"))
    } yield UDT_Email(username, domainName, domain)
}

final case class Row_HeavilyNestedUDTTable(
  id: Int,
  data: UDT_ExampleCollectionNestedUDTType
)
object Row_HeavilyNestedUDTTable {
  implicit val readerForRow_HeavilyNestedUDTTable: Reader[Row_HeavilyNestedUDTTable] =
    Reader.derive[Row_HeavilyNestedUDTTable]

  implicit val writerForRow_HeavilyNestedUDTTable: Writer[Row_HeavilyNestedUDTTable] =
    Writer.derive[Row_HeavilyNestedUDTTable]

  def gen: Gen[Random with Sized, Row_HeavilyNestedUDTTable] =
    for {
      id   <- Gen.anyInt
      data <- UDT_ExampleCollectionNestedUDTType.gen
    } yield Row_HeavilyNestedUDTTable(id, data)

  def insert(in: Row_HeavilyNestedUDTTable): CQL[MutationResult] =
    InsertBuilder("userdefinedtypesspec_heavilynestedudttable")
      .value("id", in.id)
      .value("data", in.data)
      .build

  def select(id: Int): CQL[Row_HeavilyNestedUDTTable] =
    SelectBuilder
      .from("userdefinedtypesspec_heavilynestedudttable")
      .column("id")
      .column("data")
      .where("id" === id)
      .build[Row_HeavilyNestedUDTTable]

}
final case class UDT_ExampleType(
  x: Long,
  y: Long,
  date: LocalDate,
  time: LocalTime
)
object UDT_ExampleType {
  implicit val readerForUDT_ExampleType: Reader[UDT_ExampleType] = UdtReader.deriveReader[UDT_ExampleType]
  implicit val writerForUDT_ExampleType: Writer[UDT_ExampleType] = UdtWriter.deriveWriter[UDT_ExampleType]

  def gen: Gen[Random, UDT_ExampleType] =
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
}

final case class UDT_ExampleNestedType(
  a: Int,
  b: String,
  c: UDT_ExampleType
)
object UDT_ExampleNestedType {
  implicit val readerForUDT_ExampleNestedType: Reader[UDT_ExampleNestedType] =
    UdtReader.deriveReader[UDT_ExampleNestedType]
  implicit val writerForUDT_ExampleNestedType: Writer[UDT_ExampleNestedType] =
    UdtWriter.deriveWriter[UDT_ExampleNestedType]

  def gen =
    for {
      a <- Gen.anyInt
      b <- Gen.alphaNumericStringBounded(4, 10)
      c <- UDT_ExampleType.gen
    } yield UDT_ExampleNestedType(a, b, c)
}

final case class UDT_ExampleCollectionNestedUDTType(
  a: Int,
  b: Map[Int, Set[Set[Set[Set[UDT_ExampleNestedType]]]]],
  c: UDT_ExampleNestedType
)
object UDT_ExampleCollectionNestedUDTType {
  implicit val readerForUDT_ExampleCollectionNestedUDTType: Reader[UDT_ExampleCollectionNestedUDTType] =
    UdtReader.deriveReader[UDT_ExampleCollectionNestedUDTType]
  implicit val writerForUDT_ExampleCollectionNestedUDTType: Writer[UDT_ExampleCollectionNestedUDTType] =
    UdtWriter.deriveWriter[UDT_ExampleCollectionNestedUDTType]

  def gen: Gen[Random with Sized, UDT_ExampleCollectionNestedUDTType] =
    for {
      a <- Gen.anyInt
      b <- Gen.mapOf(
             key = Gen.anyInt,
             value = Gen.setOf(
               Gen.setOf(
                 Gen.setOf(
                   Gen.setOf(
                     UDT_ExampleNestedType.gen
                   )
                 )
               )
             )
           )
      c <- UDT_ExampleNestedType.gen
    } yield UDT_ExampleCollectionNestedUDTType(a, b, c)
}
