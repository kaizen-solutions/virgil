package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.models.CursorSpecDatatypes._
import zio.test.Assertion._
import zio.test.TestAspect.samples
import zio.test._
import zio.test.scalacheck._
import zio.{test => _, _}

import java.net.InetAddress

object CursorSpec {
  def cursorSpec: Spec[Sized with TestConfig with CQLExecutor, Any] =
    suite("Cursor Specification") {
      suite("Row Cursor Specification") {
        test("Row Cursor should be able to read a complex structure") {
          check(CursorExampleRow.gen.toGenZIO) { row =>
            for {
              _              <- CursorExampleRow.truncate.execute.runDrain
              _              <- CursorExampleRow.insert(row).execute.runDrain
              result         <- CursorExampleRow.select(row.id).execute.runHead.some
              cursor          = RowCursor(result)
              resultRow      <- ZIO.fromEither(cursor.viewAs[CursorExampleRow])
              name           <- ZIO.fromEither(cursor.field[String]("name"))
              age            <- ZIO.fromEither(cursor.field[Short]("age"))
              addresses      <- ZIO.fromEither(cursor.field[List[UdtValue]]("addresses"))
              chunkAddresses <- ZIO.fromEither(cursor.field[Chunk[CursorUdtAddress]]("addresses"))
              mayBeEmpty     <- ZIO.fromEither(cursor.field[Option[String]]("may_be_empty"))
              leftMayBeEmpty  = cursor.field[String]("may_be_empty")
              udtAddress     <- ZIO.fromOption(addresses.headOption)
              addressCursor   = UdtValueCursor(udtAddress)
              address        <- ZIO.fromEither(addressCursor.viewAs[CursorUdtAddress])
              noteCursor     <- ZIO.fromEither(addressCursor.downUdtValue("note"))
              ip             <- ZIO.fromEither(noteCursor.field[InetAddress]("ip"))
            } yield assertTrue(resultRow == row) &&
              assertTrue(name == row.name) &&
              assertTrue(age == row.age) &&
              assertTrue(List(address) == row.pastAddresses) &&
              assertTrue(chunkAddresses.toList == row.pastAddresses) &&
              assertTrue(mayBeEmpty == row.mayBeEmpty) &&
              assertTrue(ip == row.pastAddresses.head.note.ip) &&
              assert(leftMayBeEmpty.left.map(_.debug))(isLeft(containsString("is not an optional field")))
          }
        }
      }
    } @@ samples(10)
}

final case class CursorExampleRowChunkVariant(
  id: Long,
  name: String,
  age: Short,
  @CqlColumn("may_be_empty") mayBeEmpty: Option[String],
  @CqlColumn("addresses") pastAddresses: Chunk[CursorUdtAddress]
)
object CursorExampleRowChunkVariant extends CursorExampleRowChunkVariantInstances
