package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import io.kaizensolutions.virgil.CqlExecutorSpecDatatypes._
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import zio.stream.ZStream
import zio.test.Assertion.hasSameElements
import zio.test.TestAspect._
import zio.test._
import zio.{test => _, _}

import java.net.InetSocketAddress

object CQLExecutorSpec {
  def executorSpec: Spec[
    Live with CQLExecutor with Clock with Random with Sized with TestConfig with CassandraContainer,
    TestFailure[Throwable],
    TestSuccess
  ] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions + configuration) @@ timeout(2.minutes) @@ samples(4)
    }

  def queries: Spec[CQLExecutor with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Queries") {
      test("selectFirst") {
        cql"SELECT now() FROM system.local"
          .query[SystemLocalResponse]
          .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
          .execute
          .runLast
          .map(result => assertTrue(result.flatMap(_.time.toOption).get > 0L))
      } +
        test("select") {
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
        test("selectPage") {
          import SelectPageRow._
          check(Gen.chunkOfN(50)(selectPageRowGen)) { actual =>
            for {
              _                             <- truncate.execute.runDrain
              _                             <- ZIO.foreachParDiscard(actual.map(insert))(_.execute.runDrain)
              attr                           = ExecutionAttributes.default.withPageSize(actual.length / 2)
              all                            = selectAll.withAttributes(attr).execute.runCollect
              paged                          = selectPageStream(selectAll.withAttributes(attr)).runCollect
              result                        <- all.zipPar(paged)
              (dataFromSelect, dataFromPage) = result
            } yield assert(dataFromPage)(hasSameElements(dataFromSelect)) &&
              assert(dataFromSelect)(hasSameElements(actual))
          }
        } +
        test("take(1)") {
          cql"SELECT * FROM system.local".query
            .take(1)
            .execute
            .runCount
            .map(rowCount => assertTrue(rowCount > 0L))
        } +
        test("take(n > 1)") {
          check(Gen.long(2, 1000)) { n =>
            cql"SELECT * FROM system.local".query
              .take(n)
              .execute
              .runCount
              .map(rowCount => assertTrue(rowCount > 0L))
          }
        }
    }

  def actions: Spec[CQLExecutor with Random with Sized with TestConfig, TestFailure[Throwable], TestSuccess] =
    suite("Actions") {
      test("executeAction") {
        import ExecuteTestTable._
        check(Gen.listOfN(10)(gen)) { actual =>
          val truncateData = truncate(table).execute.runDrain
          val toInsert     = actual.map(insert(table))
          val expected     = selectAllIn(table)(actual.map(_.id)).execute.runCollect

          for {
            _        <- truncateData
            _        <- ZIO.foreachParDiscard(toInsert)(_.execute.runDrain)
            expected <- expected
          } yield assert(actual)(hasSameElements(expected))
        }
      } +
        test("executeBatchAction") {
          import ExecuteTestTable._
          check(Gen.listOfN(10)(gen)) { actual =>
            val truncateData = truncate(batchTable).execute
            val batchedInsert: ZStream[CQLExecutor, Throwable, MutationResult] =
              actual
                .map(ExecuteTestTable.insert(batchTable))
                .reduce(_ + _)
                .batchType(BatchType.Unlogged)
                .execute

            val expected: ZStream[CQLExecutor, Throwable, ExecuteTestTable] =
              selectAllIn(batchTable)(actual.map(_.id)).execute

            for {
              _        <- truncateData.runDrain
              _        <- batchedInsert.runDrain
              expected <- expected.runCollect
            } yield assert(actual)(hasSameElements(expected))
          }
        } +
        test("executeMutation") {
          import ExecuteTestTable._
          check(gen) { data =>
            val truncateData = truncate(table).executeMutation
            val toInsert     = insert(table)(data).executeMutation
            val search       = selectAllIn(table)(data.id :: Nil).execute.runCollect
            truncateData *> toInsert *> search.map(result => assert(result)(hasSameElements(List(data))))
          }
        }
    } @@ sequential

  def configuration
    : Spec[CQLExecutor with Clock with Random with Sized with TestConfig with CassandraContainer, TestFailure[
      Throwable
    ], TestSuccess] =
    suite("Session Configuration")(
      test("Creating a layer from an existing session allows you to access Cassandra") {
        val sessionScoped: URIO[CassandraContainer with Scope, CqlSession] = {
          val createSession = for {
            c            <- ZIO.service[CassandraContainer]
            contactPoint <- (c.getHost).zipWith(c.getPort)(InetSocketAddress.createUnresolved)
            session <- ZIO.succeed(
                         CqlSession.builder
                           .addContactPoint(contactPoint)
                           .withLocalDatacenter("dc1")
                           .withKeyspace("virgil")
                           .build
                       )
          } yield session
          val releaseSession = (session: CqlSession) => ZIO.succeed(session.close())
          ZIO.acquireRelease(createSession)(releaseSession).orDie
        }

        val sessionLayer     = ZLayer.scoped(sessionScoped)
        val cqlExecutorLayer = sessionLayer >>> CQLExecutor.sessionLive

        cql"SELECT * FROM system.local".query.execute.runCount
          .map(numberOfRows => assertTrue(numberOfRows > 0L))
          .provideLayer(cqlExecutorLayer)
      },
      test("Timeouts are respected") {
        check(Gen.chunkOfN(4)(timeoutCheckRowGen)) { rows =>
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
      test("PageSize are respected and matches with chunk size") {
        check(Gen.chunkOfN(4)(pageSizeCheckRowGen)) { rows =>
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
  ): ZStream[CQLExecutor, Throwable, ScalaType] =
    ZStream
      .fromZIO(query.executePage())
      .flatMap {
        case Paged(chunk, Some(page)) =>
          ZStream.fromChunk(chunk) ++
            ZStream.paginateChunkZIO(page)(nextPage =>
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
