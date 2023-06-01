package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import com.datastax.oss.driver.api.core.data.UdtValue
import fs2.Chunk
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.models.CursorSpecDatatypes._
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

import java.net.InetAddress

class CursorSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def checkConfig: CheckConfig = CheckConfig.default.copy(minimumSuccessful = 20, perPropertyParallelism = 1)

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Row Cursor should be able to read a complex structure") { executor =>
    forall(CursorExampleRow.gen) { row =>
      for {
        _              <- executor.executeMutation(CursorExampleRow.truncate)
        _              <- executor.executeMutation(CursorExampleRow.insert(row))
        result         <- executor.execute(CursorExampleRow.select(row.id)).compile.toList.map(_.head)
        cursor          = RowCursor(result)
        resultRow      <- IO.fromEither(cursor.viewAs[CursorExampleRow])
        name           <- IO.fromEither(cursor.field[String]("name"))
        age            <- IO.fromEither(cursor.field[Short]("age"))
        addresses      <- IO.fromEither(cursor.field[List[UdtValue]]("addresses"))
        chunkAddresses <- IO.fromEither(cursor.field[Chunk[CursorUdtAddress]]("addresses"))
        mayBeEmpty     <- IO.fromEither(cursor.field[Option[String]]("may_be_empty"))
        leftMayBeEmpty  = cursor.field[String]("may_be_empty")
        udtAddress     <- IO.fromOption(addresses.headOption)(new Exception("No addresses found"))
        addressCursor   = UdtValueCursor(udtAddress)
        address        <- IO.fromEither(addressCursor.viewAs[CursorUdtAddress])
        noteCursor     <- IO.fromEither(addressCursor.downUdtValue("note"))
        ip             <- IO.fromEither(noteCursor.field[InetAddress]("ip"))
      } yield expect(resultRow == row)
        .and(expect(name == row.name))
        .and(expect(age == row.age))
        .and(expect(List(address) == row.pastAddresses))
        .and(expect(chunkAddresses.toList == row.pastAddresses))
        .and(expect(mayBeEmpty == row.mayBeEmpty))
        .and(expect(ip == row.pastAddresses.head.note.ip))
        .and(expect(leftMayBeEmpty.isLeft))
        .and(expect(leftMayBeEmpty.swap.map(_.debug).getOrElse("").contains("is not an optional field")))
    }
  }
}

trait CursorExampleRowChunkVariantInstances {}

final case class CursorExampleRowChunkVariant(
  id: Long,
  name: String,
  age: Short,
  @CqlColumn("may_be_empty") mayBeEmpty: Option[String],
  @CqlColumn("addresses") pastAddresses: Chunk[CursorUdtAddress]
)
object CursorExampleRowChunkVariant extends CursorExampleRowChunkVariantInstances
