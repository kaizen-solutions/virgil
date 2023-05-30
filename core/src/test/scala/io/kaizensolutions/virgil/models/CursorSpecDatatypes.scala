package io.kaizensolutions.virgil.models

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses
import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen

import java.net.InetAddress

object CursorSpecDatatypes {
  final case class CursorExampleRow(
    id: Long,
    name: String,
    age: Short,
    @CqlColumn("may_be_empty") mayBeEmpty: Option[String],
    @CqlColumn("addresses") pastAddresses: List[CursorUdtAddress]
  )

  object CursorExampleRow extends CursorExampleRowInstances {
    val gen: Gen[CursorExampleRow] =
      for {
        id      <- Gen.chooseNum(1L, 10000L)
        name    <- Gen.stringOf(Gen.asciiPrintableChar)
        age     <- Gen.chooseNum(1: Short, 1000: Short)
        address <- CursorUdtAddress.gen
      } yield CursorExampleRow(id, name, age, None, List(address))

    val tableName = "cursorspec_cursorexampletable"

    def truncate: CQL[MutationResult] = CQL.truncate(tableName)

    def insert(row: CursorExampleRow): CQL[MutationResult] =
      InsertBuilder(tableName)
        .values(
          "id"           -> row.id,
          "name"         -> row.name,
          "age"          -> row.age,
          "addresses"    -> row.pastAddresses,
          "may_be_empty" -> row.mayBeEmpty
        )
        .build

    def select(id: Long): CQL[Row] = {
      val cql = cql"SELECT * FROM " ++ tableName.asCql ++ cql" WHERE id = $id"
      cql.query
    }
  }

  final case class CursorUdtAddress(street: String, city: String, state: String, zip: String, note: CursorUdtNote)
  object CursorUdtAddress extends CursorUdtAddressInstances {
    val gen: Gen[CursorUdtAddress] =
      for {
        street <- Gen.stringBounded(4, 8)(Gen.alphaChar)
        city   <- Gen.stringBounded(4, 8)(Gen.alphaChar)
        state  <- Gen.stringBounded(2, 2)(Gen.alphaChar)
        zip    <- Gen.stringBounded(5, 5)(Gen.alphaChar)
        note   <- CursorUdtNote.gen
      } yield CursorUdtAddress(street = street, city = city, state = state, zip = zip, note = note)

  }

  final case class CursorUdtNote(data: String, ip: InetAddress)
  object CursorUdtNote extends CursorUdtNoteInstances {
    val gen: Gen[CursorUdtNote] =
      for {
        data         <- Gen.stringBounded(2, 4)(Gen.alphaNumChar)
        ipChunk       = Gen.chooseNum(0, 255)
        ipChunkOne   <- ipChunk
        ipChunkTwo   <- ipChunk
        ipChunkThree <- ipChunk
        ipChunkFour  <- ipChunk
        ip            = InetAddresses.forString(s"${ipChunkOne}.${ipChunkTwo}.${ipChunkThree}.${ipChunkFour}")
      } yield CursorUdtNote(data, ip)
  }
}
