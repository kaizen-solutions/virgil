package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.codecs.userdefinedtypes.UdtReader
import io.kaizensolutions.virgil.cql._
import zio.Has
import zio.duration._
import zio.random.Random
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live

object UserDefinedTypesSpec {
  def userDefinedTypesSpec: ZSpec[Live with Has[ZioCassandraSession] with Random with TestConfig, Throwable] =
    suite("User Defined Types specification") {
      testM("Write and read rows containing UDTs which are nested") {
        import Row_Person._
        checkM(Row_Person.gen) { expected =>
          val insertPeople = ZioCassandraSession.executeAction(insert(expected))
          val fetchActual  = ZioCassandraSession.selectFirst(select(expected.id))

          for {
            _      <- insertPeople
            actual <- fetchActual
          } yield assertTrue(actual.get == expected)
        }
      }
    } @@ timeout(30.seconds) @@ samples(50)
}

final case class Row_Person(
  id: Int,
  name: String,
  age: Int,
  data: UDT_Data
)
object Row_Person {
  implicit val readerForRowPerson: Reader[Row_Person] = Reader.derive[Row_Person]

  def insert(person: Row_Person): Action.Single =
    cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".action

  def select(id: Int): Query[Row_Person] =
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
