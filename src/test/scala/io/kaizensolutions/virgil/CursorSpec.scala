package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses
import io.kaizensolutions.virgil.CursorSpecDatatypes._
import zio.test.Assertion._
import zio.test.TestAspect.samples
import zio.test._
import zio.{test => _, _}

import java.net.InetAddress

object CursorSpec {
  def cursorSpec =
    suite("Cursor Specification") {
      suite("Row Cursor Specification") {
        test("Row Cursor should be able to read a complex structure") {
          check(cursorExampleRowGen) { row =>
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

  def cursorExampleRowGen: Gen[Random with Sized, CursorExampleRow] =
    for {
      id      <- Gen.long(1, 10000)
      name    <- Gen.string
      age     <- Gen.short
      address <- cursorUdtAddressGen
    } yield CursorExampleRow(id, name, age, None, Chunk(address))

  def cursorUdtAddressGen: Gen[Random, CursorUdtAddress] =
    for {
      street <- Gen.stringBounded(4, 8)(Gen.alphaChar)
      city   <- Gen.stringBounded(4, 8)(Gen.alphaChar)
      state  <- Gen.stringBounded(2, 2)(Gen.alphaChar)
      zip    <- Gen.stringBounded(5, 5)(Gen.alphaChar)
      note   <- cursorUdtNoteGen
    } yield CursorUdtAddress(street = street, city = city, state = state, zip = zip, note = note)

  def cursorUdtNoteGen: Gen[Random, CursorUdtNote] =
    for {
      data    <- Gen.stringBounded(2, 4)(Gen.alphaNumericChar)
      ipChunk <- Gen.int(0, 255)
      ip       = InetAddresses.forString(s"${ipChunk}.${ipChunk}.${ipChunk}.${ipChunk}")
    } yield CursorUdtNote(data, ip)
}
