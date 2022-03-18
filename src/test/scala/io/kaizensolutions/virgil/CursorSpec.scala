package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl.InsertBuilder
import zio.random.Random
import zio.test.TestAspect.samples
import zio.test._
import zio.test.Assertion._
import zio.{Chunk, ZIO}

import java.net.InetAddress

object CursorSpec {
  def cursorSpec =
    suite("Cursor Specification") {
      suite("Row Cursor Specification") {
        testM("Row Cursor should be able to read a complex structure") {
          checkM(CursorExampleRow.gen) { row =>
            for {
              _             <- CursorExampleRow.truncate.execute.runDrain
              _             <- CursorExampleRow.insert(row).execute.runDrain
              result        <- CursorExampleRow.select(row.id).execute.runHead.some
              cursor         = RowCursor(result)
              resultRow     <- ZIO.fromEither(cursor.viewAs[CursorExampleRow])
              name          <- ZIO.fromEither(cursor.field[String]("name"))
              age           <- ZIO.fromEither(cursor.field[Short]("age"))
              addresses     <- ZIO.fromEither(cursor.field[List[UdtValue]]("addresses"))
              mayBeEmpty    <- ZIO.fromEither(cursor.field[Option[String]]("may_be_empty"))
              leftMayBeEmpty = cursor.field[String]("may_be_empty")
              udtAddress    <- ZIO.fromOption(addresses.headOption)
              addressCursor  = UdtValueCursor(udtAddress)
              address       <- ZIO.fromEither(addressCursor.viewAs[CursorUdtAddress])
              noteCursor    <- ZIO.fromEither(addressCursor.downUdtValue("note"))
              ip            <- ZIO.fromEither(noteCursor.field[InetAddress]("ip"))
            } yield assertTrue(resultRow == row) &&
              assertTrue(name == row.name) &&
              assertTrue(age == row.age) &&
              assertTrue(Chunk(address) == row.pastAddresses) &&
              assertTrue(mayBeEmpty == row.mayBeEmpty) &&
              assertTrue(ip == row.pastAddresses.head.note.ip) &&
              assert(leftMayBeEmpty.left.map(_.debug))(isLeft(containsString("is not an optional field")))
          }
        }
      }
    } @@ samples(10)
}

final case class CursorExampleRow(
  id: Long,
  name: String,
  age: Short,
  @CqlColumn("may_be_empty") mayBeEmpty: Option[String],
  @CqlColumn("addresses") pastAddresses: Chunk[CursorUdtAddress]
)
object CursorExampleRow {
  val tableName                     = "cursorspec_cursorexampletable"
  def truncate: CQL[MutationResult] = CQL.truncate(tableName)

  def insert(row: CursorExampleRow): CQL[MutationResult] =
    InsertBuilder(tableName)
      .value("id", row.id)
      .value("name", row.name)
      .value("age", row.age)
      .value("addresses", row.pastAddresses)
      .value("may_be_empty", row.mayBeEmpty)
      .build

  def select(id: Long): CQL[Row] = {
    val cql = cql"SELECT * FROM " ++ tableName.asCql ++ cql" WHERE id = $id"
    cql.query
  }

  def gen: Gen[Random with Sized, CursorExampleRow] =
    for {
      id      <- Gen.long(1, 10000)
      name    <- Gen.anyString
      age     <- Gen.anyShort
      address <- CursorUdtAddress.gen
    } yield CursorExampleRow(id, name, age, None, Chunk(address))
}

final case class CursorUdtAddress(street: String, city: String, state: String, zip: String, note: CursorUdtNote)
object CursorUdtAddress {
  def gen: Gen[Random, CursorUdtAddress] =
    for {
      street <- Gen.stringBounded(4, 8)(Gen.alphaChar)
      city   <- Gen.stringBounded(4, 8)(Gen.alphaChar)
      state  <- Gen.stringBounded(2, 2)(Gen.alphaChar)
      zip    <- Gen.stringBounded(5, 5)(Gen.alphaChar)
      note   <- CursorUdtNote.gen
    } yield CursorUdtAddress(street = street, city = city, state = state, zip = zip, note = note)
}

final case class CursorUdtNote(data: String, ip: InetAddress)
object CursorUdtNote {
  def gen: Gen[Random, CursorUdtNote] =
    for {
      data    <- Gen.stringBounded(2, 4)(Gen.alphaNumericChar)
      ipChunk <- Gen.int(0, 255)
      ip       = InetAddresses.forString(s"${ipChunk}.${ipChunk}.${ipChunk}.${ipChunk}")
    } yield CursorUdtNote(data, ip)
}
