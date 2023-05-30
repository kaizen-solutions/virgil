package io.kaizensolutions.virgil.models

import com.datastax.oss.driver.api.core.uuid.Uuids
import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

object CqlExecutorSpecDatatypes {
  final case class SystemLocalResponse(@CqlColumn("system.now()") now: UUID) {
    def time: Either[Throwable, Long] =
      Try(Uuids.unixTimestamp(now)).toEither
  }
  object SystemLocalResponse extends SystemLocalResponseInstances

  final case class PreparedStatementsResponse(
    @CqlColumn("prepared_id") preparedId: ByteBuffer,
    @CqlColumn("logged_keyspace") keyspace: Option[String],
    @CqlColumn("query_string") query: String
  )
  object PreparedStatementsResponse extends PreparedStatementsResponseInstances
  final case class ExecuteTestTable(id: Int, info: String)
  object ExecuteTestTable extends ExecuteTestTableInstances {
    implicit val orderingForExecuteTable: Ordering[ExecuteTestTable] =
      Ordering.by[ExecuteTestTable, (Int, String)](o => (o.id, o.info))

    val table      = "ziocassandrasessionspec_executeAction"
    val batchTable = "ziocassandrasessionspec_executeBatchAction"

    def truncate(tbl: String): CQL[MutationResult] =
      CQL
        .truncate(tbl)
        .withAttributes(
          ExecutionAttributes.default
            .withConsistencyLevel(ConsistencyLevel.All)
        )

    val gen: Gen[ExecuteTestTable] = for {
      id   <- Gen.chooseNum(1, 1000)
      info <- Gen.stringBounded(10, 15)(Gen.alphaNumChar)
    } yield ExecuteTestTable(id, info)

    def insert(table: String)(in: ExecuteTestTable): CQL[MutationResult] =
      (cql"INSERT INTO ".appendString(table) ++ cql"(id, info) VALUES (${in.id}, ${in.info})").mutation

    def selectAllIn(table: String)(ids: Iterable[Int]): CQL[ExecuteTestTable] =
      (cql"SELECT id, info FROM ".appendString(table) ++ cql" WHERE id IN ${ids.toList}")
        .query[ExecuteTestTable]
  }

  final case class SelectPageRow(id: Int, bucket: Int, info: String)
  object SelectPageRow extends SelectPageRowInstances {
    val gen: Gen[SelectPageRow] =
      for {
        id     <- Gen.chooseNum(1, 1000)
        bucket <- Gen.chooseNum(1, 50)
        info   <- Gen.stringBounded(10, 15)(Gen.alphaNumChar)
      } yield SelectPageRow(id, bucket, info)

    val truncate: CQL[MutationResult] = CQL.truncate("ziocassandrasessionspec_selectPage")

    def insert(in: SelectPageRow): CQL[MutationResult] =
      cql"INSERT INTO ziocassandrasessionspec_selectPage (id, bucket, info) VALUES (${in.id}, ${in.bucket}, ${in.info})".mutation

    def selectAll: CQL[SelectPageRow] =
      cql"SELECT id, bucket, info FROM ziocassandrasessionspec_selectPage".query[SelectPageRow]
  }

  final case class TimeoutCheckRow(id: Int, info: String, @CqlColumn("another_info") anotherInfo: String)
  object TimeoutCheckRow extends TimeoutCheckRowInstances {
    val gen: Gen[TimeoutCheckRow] = for {
      id          <- Gen.chooseNum(1, 1000)
      info        <- Gen.stringBounded(100, 150)(Gen.alphaNumChar)
      anotherInfo <- Gen.stringBounded(200, 400)(Gen.alphaNumChar)
    } yield TimeoutCheckRow(id, info, anotherInfo)

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
  object PageSizeCheckRow extends PageSizeCheckRowInstances {
    val gen: Gen[PageSizeCheckRow] = for {
      id   <- Gen.chooseNum(1, 1000)
      info <- Gen.stringBounded(100, 150)(Gen.alphaNumChar)
    } yield PageSizeCheckRow(id, info)

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
