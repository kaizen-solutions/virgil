package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.uuid.Uuids
import io.kaizensolutions.virgil.annotations.CqlColumn
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

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID
import scala.util.Try

object CQLExecutorSpec {
  def sessionSpec
    : Spec[Live with Has[CassandraContainer] with Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[
      Throwable
    ], TestSuccess] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions + configuration) @@ timeout(1.minute) @@ samples(10)
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
                import r._

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
              _                             <- truncate.execute.runDrain
              _                             <- ZIO.foreachPar_(actual.map(insert))(_.execute.runDrain)
              attr                           = ExecutionAttributes.default.withPageSize(actual.length / 2)
              all                            = selectAll.withAttributes(attr).execute.runCollect
              paged                          = selectPageStream(selectAll.withAttributes(attr)).runCollect
              result                        <- all.zipPar(paged)
              (dataFromSelect, dataFromPage) = result
            } yield assert(dataFromPage)(hasSameElements(dataFromSelect)) &&
              assert(dataFromSelect)(hasSameElements(actual))
          }
        } +
        testM("take(1)") {
          cql"SELECT * FROM system.local".query
            .take(1)
            .execute
            .runCount
            .map(rowCount => assertTrue(rowCount > 0))
        } +
        testM("take(n > 1)") {
          checkM(Gen.long(2, 1000)) { n =>
            cql"SELECT * FROM system.local".query
              .take(n)
              .execute
              .runCount
              .map(rowCount => assertTrue(rowCount > 0))
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

  def configuration: Spec[Has[CassandraContainer], TestFailure[Throwable], TestSuccess] =
    suite("Session Configuration") {
      testM("Creating a layer from an existing session allows you to access Cassandra") {
        val sessionManaged: URManaged[Has[CassandraContainer], CqlSession] = {
          val createSession = for {
            c            <- ZIO.service[CassandraContainer]
            contactPoint <- (c.getHost).zipWith(c.getPort)(InetSocketAddress.createUnresolved)
            session <- ZIO.effectTotal(
                         CqlSession.builder
                           .addContactPoint(contactPoint)
                           .withLocalDatacenter("dc1")
                           .withKeyspace("virgil")
                           .build
                       )
          } yield session
          val releaseSession = (session: CqlSession) => ZIO.effectTotal(session.close())
          ZManaged.make(createSession)(releaseSession).orDie
        }

        val sessionLayer     = ZLayer.fromManaged(sessionManaged)
        val cqlExecutorLayer = sessionLayer >>> CQLExecutor.sessionLive

        cql"SELECT * FROM system.local".query.execute.runCount
          .map(numberOfRows => assertTrue(numberOfRows > 0))
          .provideLayer(cqlExecutorLayer)
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

final case class SystemLocalResponse(
  @CqlColumn("system.now()") now: UUID
) {
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

  val gen: Gen[Random with Sized, SelectPageRow] =
    for {
      id     <- Gen.int(1, 1000)
      bucket <- Gen.int(1, 50)
      info   <- Gen.alphaNumericStringBounded(10, 15)
    } yield SelectPageRow(id, bucket, info)
}
