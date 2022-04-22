package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.uuid.Uuids
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import zio.random.Random
import zio.test.{Gen, Sized}

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

object CqlExecutorSpecDatatypes {
  final case class SystemLocalResponse(@CqlColumn("system.now()") now: UUID) {
    def time: Either[Throwable, Long] =
      Try(Uuids.unixTimestamp(now)).toEither
  }
  final case class PreparedStatementsResponse(
    @CqlColumn("prepared_id") preparedId: ByteBuffer,
    @CqlColumn("logged_keyspace") keyspace: Option[String],
    @CqlColumn("query_string") query: String
  )

  final case class ExecuteTestTable(id: Int, info: String)
  object ExecuteTestTable {
    val table      = "ziocassandrasessionspec_executeAction"
    val batchTable = "ziocassandrasessionspec_executeBatchAction"

    def truncate(tbl: String): CQL[MutationResult] = CQL.truncate(tbl)

    val gen: Gen[Random with Sized, ExecuteTestTable] = for {
      id   <- Gen.int(1, 1000)
      info <- Gen.alphaNumericStringBounded(10, 15)
    } yield ExecuteTestTable(id, info)

    def insert(table: String)(in: ExecuteTestTable): CQL[MutationResult] =
      (cql"INSERT INTO ".appendString(table) ++ cql"(id, info) VALUES (${in.id}, ${in.info})").mutation

    def selectAllIn(table: String)(ids: List[Int]): CQL[ExecuteTestTable] =
      (cql"SELECT id, info FROM ".appendString(table) ++ cql" WHERE id IN $ids")
        .query[ExecuteTestTable]
  }

  final case class SelectPageRow(id: Int, bucket: Int, info: String)
  object SelectPageRow {
    val truncate: CQL[MutationResult] = CQL.truncate("ziocassandrasessionspec_selectPage")

    def insert(in: SelectPageRow): CQL[MutationResult] =
      cql"INSERT INTO ziocassandrasessionspec_selectPage (id, bucket, info) VALUES (${in.id}, ${in.bucket}, ${in.info})".mutation

    def selectAll: CQL[SelectPageRow] =
      cql"SELECT id, bucket, info FROM ziocassandrasessionspec_selectPage".query[SelectPageRow]
  }

  final case class TimeoutCheckRow(id: Int, info: String, @CqlColumn("another_info") anotherInfo: String)
  object TimeoutCheckRow {
    val table = "ziocassandrasessionspec_timeoutcheck"

    val selectAll: CQL[TimeoutCheckRow] =
      s"SELECT id, info, another_info FROM $table".asCql.query[TimeoutCheckRow]

    def insert(in: TimeoutCheckRow): CQL[MutationResult] =
      InsertBuilder(table)
        .value("id", in.id)
        .value("info", in.info)
        .value("another_info", in.anotherInfo)
        .build
  }

  final case class PageSizeCheckRow(id: Int, info: String)
  object PageSizeCheckRow {
    val table = "ziocassandrasessionspec_pageSizeCheck"

    val selectAll: CQL[PageSizeCheckRow] =
      s"SELECT id, info FROM $table".asCql.query[PageSizeCheckRow]

    def insert(in: PageSizeCheckRow): CQL[MutationResult] =
      InsertBuilder(table)
        .value("id", in.id)
        .value("info", in.info)
        .build
  }
}
