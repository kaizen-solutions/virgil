package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.uuid.Uuids
import io.kaizensolutions.virgil.codecs.CqlDecoder
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import zio._
import zio.duration._
import zio.random.Random
import zio.stream.ZStream
import zio.test.Assertion.hasSameElements
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live

import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

object CQLExecutorSpec {
  def sessionSpec: Spec[Live with Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[
    Throwable
  ], TestSuccess] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions) @@ timeout(1.minute) @@ samples(10)
    }

  def queries: Spec[Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Queries") {
      testM("selectFirst") {
        cql"SELECT now() FROM system.local"
          .query[SystemLocalResponse]
          .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
          .execute
          .runLast
          .map(result => assertTrue(result.flatMap(_.time.toOption).get > 0))
      } +
        testM("select") {
          cql"SELECT prepared_id, logged_keyspace, query_string FROM system.prepared_statements"
            .query[PreparedStatementsResponse]
            .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
            .execute
            .runCollect
            .map(results =>
              assertTrue(results.forall { r =>
                import r.{query_string => query}

                query.contains("SELECT") ||
                query.contains("UPDATE") ||
                query.contains("CREATE") ||
                query.contains("DELETE") ||
                query.contains("INSERT") ||
                query.contains("USE")
              })
            )
        } +
        testM("selectPage") {
          import SelectPageRow._
          checkM(Gen.chunkOfN(50)(gen)) { actual =>
            for {
              _  <- truncate.execute.runDrain
              _  <- ZIO.foreachPar_(actual.map(insert))(_.execute.runDrain)
              all = selectAll.execute.runCollect
              paged =
                selectPageStream(
                  selectAll
                    .withAttributes(ExecutionAttributes.default.withPageSize(actual.length / 2))
                ).runCollect
              result                        <- all.zipPar(paged)
              (dataFromSelect, dataFromPage) = result
            } yield assert(dataFromPage)(hasSameElements(dataFromSelect)) &&
              assert(dataFromSelect)(hasSameElements(actual))
          }
        }
    }

  def actions: Spec[Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Actions") {
      testM("executeAction") {
        import ExecuteTestTable._
        checkM(Gen.listOfN(10)(gen)) { actual =>
          val truncateData = truncate(table).execute.runDrain
          val toInsert     = actual.map(insert(table))
          val expected     = selectAllIn(table)(actual.map(_.id)).execute.runCollect

          for {
            _        <- truncateData
            _        <- ZIO.foreachPar_(toInsert)(_.execute.runDrain)
            expected <- expected
          } yield assert(actual)(hasSameElements(expected))
        }
      } +
        testM("executeBatchAction") {
          import ExecuteTestTable._
          checkM(Gen.listOfN(10)(gen)) { actual =>
            val truncateData = truncate(batchTable).execute
            val batchedInsert: ZStream[Has[CQLExecutor], Throwable, MutationResult] =
              actual
                .map(ExecuteTestTable.insert(batchTable))
                .reduce(_ + _)
                .batchType(BatchType.Unlogged)
                .execute

            val expected: ZStream[Has[CQLExecutor], Throwable, ExecuteTestTable] =
              selectAllIn(batchTable)(actual.map(_.id)).execute

            for {
              _        <- truncateData.runDrain
              _        <- batchedInsert.runDrain
              expected <- expected.runCollect
            } yield assert(actual)(hasSameElements(expected))
          }
        }
    }

  // Used to provide a similar API as the `select` method
  private def selectPageStream[ScalaType](
    query: CQL[ScalaType]
  ): ZStream[Has[CQLExecutor], Throwable, ScalaType] =
    ZStream
      .fromEffect(query.executePage())
      .flatMap {
        case Paged(chunk, Some(page)) =>
          ZStream.fromChunk(chunk) ++
            ZStream.paginateChunkM(page)(nextPage =>
              query
                .executePage(Some(nextPage))
                .map(r => (r.data, r.pageState))
            )

        case Paged(chunk, None) =>
          ZStream.fromChunk(chunk)
      }
}

final case class SystemLocalResponse(`system.now()`: UUID) {
  def time: Either[Throwable, Long] =
    Try(Uuids.unixTimestamp(`system.now()`)).toEither
}
object SystemLocalResponse {
  implicit val decoderForSystemLocalResponse: CqlDecoder[SystemLocalResponse] =
    CqlDecoder.derive[SystemLocalResponse]
}

final case class PreparedStatementsResponse(
  prepared_id: ByteBuffer,
  logged_keyspace: Option[String],
  query_string: String
)
object PreparedStatementsResponse {
  implicit val decoderForPreparedStatementsResponse: CqlDecoder[PreparedStatementsResponse] =
    CqlDecoder.derive[PreparedStatementsResponse]
}

final case class ExecuteTestTable(id: Int, info: String)
object ExecuteTestTable {
  implicit val decoderForExecuteTestTable: CqlDecoder[ExecuteTestTable] =
    CqlDecoder.derive[ExecuteTestTable]

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
  implicit val decoderForSelectPageRow: CqlDecoder[SelectPageRow] =
    CqlDecoder.derive[SelectPageRow]

  val truncate: CQL[MutationResult] = CQL.truncate("ziocassandrasessionspec_selectPage")

  def insert(in: SelectPageRow): CQL[MutationResult] =
    cql"INSERT INTO ziocassandrasessionspec_selectPage (id, bucket, info) VALUES (${in.id}, ${in.bucket}, ${in.info})".mutation

  def selectAll: CQL[SelectPageRow] =
    cql"SELECT id, bucket, info FROM ziocassandrasessionspec_selectPage".query[SelectPageRow]

  val gen: Gen[Random with Sized, SelectPageRow] =
    for {
      id     <- Gen.int(1, 1000)
      bucket <- Gen.int(1, 50)
      info   <- Gen.alphaNumericStringBounded(10, 15)
    } yield SelectPageRow(id, bucket, info)
}
