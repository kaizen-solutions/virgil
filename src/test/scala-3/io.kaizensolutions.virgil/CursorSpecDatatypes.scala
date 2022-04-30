package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql.*
import io.kaizensolutions.virgil.dsl.*
import io.kaizensolutions.virgil.codecs.*
import zio.Random
import zio.test.*
import zio.Chunk
import java.net.InetAddress

object CursorSpecDatatypes {
  final case class CursorExampleRow(
    id: Long,
    name: String,
    age: Short,
    @CqlColumn("may_be_empty") mayBeEmpty: Option[String],
    @CqlColumn("addresses") pastAddresses: Chunk[CursorUdtAddress]
  )

  object CursorExampleRow {
    given cqlRowDecoderForCursorExampleRow: CqlRowDecoder.Object[CursorExampleRow] =
      CqlRowDecoder.derive[CursorExampleRow]

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
  object CursorUdtAddress {
    given cqlUdtValueDecoderForCursorUdtAddress: CqlUdtValueDecoder.Object[CursorUdtAddress] =
      CqlUdtValueDecoder.derive[CursorUdtAddress]

    given cqlUdtValueEncoderForCursorUdtAddress: CqlUdtValueEncoder.Object[CursorUdtAddress] =
      CqlUdtValueEncoder.derive[CursorUdtAddress]
  }

  final case class CursorUdtNote(data: String, ip: InetAddress)
  object CursorUdtNote {
    given cqlUdtValueDecoderForCursorUdtNote: CqlUdtValueDecoder.Object[CursorUdtNote] =
      CqlUdtValueDecoder.derive[CursorUdtNote]

    given cqlUdtValueEncoderForCursorUdtNote: CqlUdtValueEncoder.Object[CursorUdtNote] =
      CqlUdtValueEncoder.derive[CursorUdtNote]
  }
}
