package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import io.kaizensolutions.virgil.CqlExecutorSpecDatatypes._
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import zio._
import zio.clock.Clock
import zio.duration._
import zio.random.Random
import zio.stream.ZStream
import zio.test.Assertion.hasSameElements
import zio.test.TestAspect._
import zio.test._
import zio.test.environment.Live

import java.net.InetSocketAddress

object CQLExecutorSpec {
  def executorSpec: Spec[
    Live with Has[CQLExecutor] with Clock with Random with Sized with TestConfig with Has[CassandraContainer],
    TestFailure[Throwable],
    TestSuccess
  ] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions + configuration) @@ timeout(2.minutes) @@ samples(4)
    }

  def queries: Spec[Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Queries") {
      testM("selectFirst") {
        cql"SELECT now() FROM system.local"
          .query[SystemLocalResponse]
          .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
          .execute
          .runLast
          .map(result => assertTrue(result.flatMap(_.time.toOption).get > 0L))
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
          checkM(Gen.chunkOfN(50)(selectPageRowGen)) { actual =>
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
            .map(rowCount => assertTrue(rowCount > 0L))
        } +
        testM("take(n > 1)") {
          checkM(Gen.long(2, 1000)) { n =>
            cql"SELECT * FROM system.local".query
              .take(n)
              .execute
              .runCount
              .map(rowCount => assertTrue(rowCount > 0L))
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
        } +
        testM("executeMutation") {
          import ExecuteTestTable._
          checkM(gen) { data =>
            val truncateData = truncate(table).executeMutation
            val toInsert     = insert(table)(data).executeMutation
            val search       = selectAllIn(table)(data.id :: Nil).execute.runCollect
            truncateData *> toInsert *> search.map(result => assert(result)(hasSameElements(List(data))))
          }
        }
    } @@ sequential

  def configuration
    : Spec[Has[CQLExecutor] with Clock with Random with Sized with TestConfig with Has[CassandraContainer], TestFailure[
      Throwable
    ], TestSuccess] =
    suite("Session Configuration")(
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
          .map(numberOfRows => assertTrue(numberOfRows > 0L))
          .provideLayer(cqlExecutorLayer)
      },
      testM("Timeouts are respected") {
        checkM(Gen.chunkOfN(4)(timeoutCheckRowGen)) { rows =>
          val insert =
            ZStream
              .fromIterable(rows)
              .map(TimeoutCheckRow.insert)
              .timeout(4.seconds)
              .flatMap(_.execute)

          val select =
            TimeoutCheckRow.selectAll
              .timeout(2.second)
              .execute
              .runCount

          (insert.runDrain *> select)
            .map(c => assertTrue(c == rows.length.toLong))
        }
      } @@ samples(1),
      testM("PageSize are respected and matches with chunk size") {
        checkM(Gen.chunkOfN(4)(pageSizeCheckRowGen)) { rows =>
          val insert =
            ZStream
              .fromIterable(rows)
              .map(PageSizeCheckRow.insert)
              .map(_.timeout(4.seconds))
              .flatMap(_.execute)

          val select =
            PageSizeCheckRow.selectAll
              .pageSize(2)
              .timeout(2.second)
              .execute
              .mapChunks(s => Chunk.single(s.size))
              .runCollect

          (insert.runDrain *> select)
            .map(c => assertTrue(c.size == 2 && c.forall(_ == 2)))
        }
      } @@ samples(1) @@ shrinks(0)
    )

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

  val selectPageRowGen: Gen[Random with Sized, SelectPageRow] =
    for {
      id     <- Gen.int(1, 1000)
      bucket <- Gen.int(1, 50)
      info   <- Gen.alphaNumericStringBounded(10, 15)
    } yield SelectPageRow(id, bucket, info)

  val timeoutCheckRowGen: Gen[Random with Sized, TimeoutCheckRow] = for {
    id          <- Gen.int(1, 1000)
    info        <- Gen.alphaNumericStringBounded(100, 150)
    anotherInfo <- Gen.alphaNumericStringBounded(200, 400)
  } yield TimeoutCheckRow(id, info, anotherInfo)

  val pageSizeCheckRowGen: Gen[Random with Sized, PageSizeCheckRow] = for {
    id   <- Gen.int(1, 1000)
    info <- Gen.alphaNumericStringBounded(100, 150)
  } yield PageSizeCheckRow(id, info)
}
