package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen
import zio.test.Sized
import zio.test.{Gen => ZGen}

import java.time.LocalDate
import java.time.LocalTime
import java.util.UUID

object UserDefinedTypesSpecDatatypes {
  final case class Row_Person(
    id: Int,
    name: String,
    age: Int,
    data: UDT_Data
  )

  object Row_Person extends Row_PersonInstances {
    val zioGen: ZGen[Any, Row_Person] =
      for {
        id   <- ZGen.int(1, 100000)
        name <- ZGen.stringBounded(4, 10)(ZGen.alphaNumericChar)
        age  <- ZGen.int(18, 100)
        data <- UDT_Data.zioGen
      } yield Row_Person(id, name, age, data)

    val gen: Gen[Row_Person] =
      for {
        id   <- Gen.uuid.map(_.hashCode().abs.toInt)
        name <- Gen.stringBounded(4, 10)(Gen.alphaNumChar)
        age  <- Gen.chooseNum(18, 100)
        data <- UDT_Data.gen
      } yield Row_Person(id, name, age, data)

    def insert(person: Row_Person): CQL[MutationResult] =
      cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".mutation

    def select(id: Int): CQL[Row_Person] =
      cql"SELECT id, name, age, data FROM userdefinedtypesspec_person WHERE id = $id".query[Row_Person]

    def truncate: CQL[MutationResult] =
      cql"TRUNCATE userdefinedtypesspec_person".mutation
  }

  final case class UDT_Data(
    addresses: List[UDT_Address],
    email: Option[UDT_Email]
  )
  object UDT_Data extends UDT_DataInstances {
    val zioGen: ZGen[Any, UDT_Data] =
      for {
        addresses <- ZGen.listOfBounded(10, 20)(UDT_Address.zioGen)
        email     <- ZGen.option(UDT_Email.zioGen)
      } yield UDT_Data(addresses, email)

    val gen: Gen[UDT_Data] =
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
  object UDT_Address extends UDT_AddressInstances {
    val zioGen: ZGen[Any, UDT_Address] =
      for {
        number <- ZGen.int(1, 10000)
        street <- ZGen.stringBounded(4, 10)(ZGen.alphaNumericChar)
        city   <- ZGen.stringBounded(4, 10)(ZGen.alphaNumericChar)
      } yield UDT_Address(number, street, city)

    val gen: Gen[UDT_Address] =
      for {
        number <- Gen.chooseNum(1, 10000)
        street <- Gen.stringBounded(4, 10)(Gen.alphaNumChar)
        city   <- Gen.stringBounded(4, 10)(Gen.alphaNumChar)
      } yield UDT_Address(number, street, city)
  }

  final case class UDT_Email(
    username: String,
    @CqlColumn("domain_name") domainName: String,
    domain: String
  )
  object UDT_Email extends UDT_EmailInstances {
    val zioGen: ZGen[Any, UDT_Email] =
      for {
        username   <- ZGen.stringBounded(3, 10)(ZGen.alphaNumericChar)
        domainName <- ZGen.stringBounded(4, 32)(ZGen.alphaNumericChar)
        domain     <- ZGen.oneOf(ZGen.const("com"), ZGen.const("org"), ZGen.const("net"))
      } yield UDT_Email(username, domainName, domain)

    val gen: Gen[UDT_Email] =
      for {
        username   <- Gen.stringBounded(3, 10)(Gen.alphaNumChar)
        domainName <- Gen.stringBounded(4, 32)(Gen.alphaNumChar)
        domain     <- Gen.oneOf(Gen.const("com"), Gen.const("org"), Gen.const("net"))
      } yield UDT_Email(username, domainName, domain)
  }

  final case class Row_HeavilyNestedUDTTable(
    id: Int,
    data: UDT_ExampleCollectionNestedUDTType
  )
  object Row_HeavilyNestedUDTTable extends Row_HeavilyNestedUDTTableInstances {
    val zioGen: ZGen[Sized, Row_HeavilyNestedUDTTable] =
      for {
        id   <- ZGen.int
        data <- UDT_ExampleCollectionNestedUDTType.zioGen
      } yield Row_HeavilyNestedUDTTable(id, data)

    val sample: Row_HeavilyNestedUDTTable =
      Row_HeavilyNestedUDTTable(UUID.randomUUID().hashCode().abs.toInt, UDT_ExampleCollectionNestedUDTType.sample)

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
  object UDT_ExampleType extends UDT_ExampleTypeInstances {
    val zioGen: ZGen[Any, UDT_ExampleType] =
      for {
        x <- ZGen.long
        y <- ZGen.long
        // Interesting note: the Java date and time library can express a range of dates and times far greater than what Cassandra supports
        day    <- ZGen.int(1, 28)
        month  <- ZGen.int(1, 12)
        year   <- ZGen.int(1999, 2050)
        hour   <- ZGen.int(0, 23)
        minute <- ZGen.int(0, 59)
        date   <- ZGen.const(LocalDate.of(year, month, day))
        time   <- ZGen.oneOf(ZGen.const(LocalTime.of(hour, minute)))
      } yield UDT_ExampleType(
        x = x,
        y = y,
        date = date,
        time = time
      )

    val gen: Gen[UDT_ExampleType] =
      for {
        x <- Gen.long
        y <- Gen.long
        // Interesting note: the Java date and time library can express a range of dates and times far greater than what Cassandra supports
        day    <- Gen.chooseNum(1, 28)
        month  <- Gen.chooseNum(1, 12)
        year   <- Gen.chooseNum(1999, 2050)
        hour   <- Gen.chooseNum(0, 23)
        minute <- Gen.chooseNum(0, 59)
        date   <- Gen.lzy(Gen.const(LocalDate.of(year, month, day)))
        time   <- Gen.lzy(Gen.const(LocalTime.of(hour, minute)))
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
  object UDT_ExampleNestedType extends UDT_ExampleNestedTypeInstances {
    val zioGen: ZGen[Sized, UDT_ExampleNestedType] =
      for {
        a <- ZGen.int
        b <- ZGen.alphaNumericStringBounded(4, 10)
        c <- UDT_ExampleType.zioGen
      } yield UDT_ExampleNestedType(a, b, c)

    val gen: Gen[UDT_ExampleNestedType] =
      for {
        a <- Gen.int
        b <- Gen.stringBounded(4, 10)(Gen.alphaNumChar)
        c <- UDT_ExampleType.gen
      } yield UDT_ExampleNestedType(a, b, c)
  }

  final case class UDT_ExampleCollectionNestedUDTType(
    a: Int,
    b: Map[Int, Set[Set[Set[Set[UDT_ExampleNestedType]]]]],
    c: UDT_ExampleNestedType
  )
  object UDT_ExampleCollectionNestedUDTType extends UDT_ExampleCollectionNestedUDTTypeInstances {
    val zioGen: ZGen[Sized, UDT_ExampleCollectionNestedUDTType] =
      for {
        a <- ZGen.int
        b <- ZGen.mapOf(
               key = ZGen.int,
               value = ZGen.setOf(
                 ZGen.setOf(
                   ZGen.setOf(
                     ZGen.setOf(UDT_ExampleNestedType.zioGen)
                   )
                 )
               )
             )
        c <- UDT_ExampleNestedType.zioGen
      } yield UDT_ExampleCollectionNestedUDTType(a, b, c)

    val sample: UDT_ExampleCollectionNestedUDTType =
      UDT_ExampleCollectionNestedUDTType(
        a = 1,
        b = Map(
          1 -> Set(
            Set(
              Set(
                Set(
                  UDT_ExampleNestedType(
                    a = 1,
                    b = "a",
                    c = UDT_ExampleType(
                      x = 1,
                      y = 1,
                      date = LocalDate.of(2020, 1, 1),
                      time = LocalTime.of(1, 1)
                    )
                  )
                )
              )
            )
          )
        ),
        c = UDT_ExampleNestedType(
          a = 1,
          b = "a",
          c = UDT_ExampleType(
            x = 1,
            y = 1,
            date = LocalDate.of(2020, 1, 1),
            time = LocalTime.of(1, 1)
          )
        )
      )

  }
}
