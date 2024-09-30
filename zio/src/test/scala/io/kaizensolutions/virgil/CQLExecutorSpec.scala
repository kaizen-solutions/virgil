package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.kaizensolutions.virgil.configuration.ConsistencyLevel
import io.kaizensolutions.virgil.configuration.ExecutionAttributes
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._
import zio.test.scalacheck._
import zio.{test => _, _}

object CQLExecutorSpec {
  def executorSpec: Spec[Live & TestConfig & Sized & CqlSessionBuilder & CQLExecutor, Any] =
    suite("Cassandra Session Interpreter Specification") {
      (queries + actions + configuration) @@ timeout(2.minutes) @@ samples(4)
    }

  def queries: Spec[Sized & CQLExecutor, Throwable] =
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
          // id is the primary key
          val testGen = Gen.chunkOfN(50)(gen.toGenZIO).map(distinctBy(_.id))
          check(testGen) { actual =>
            for {
              _                             <- truncate.execute.runDrain
              _                             <- ZIO.foreachParDiscard(actual.map(insert))(_.execute.runDrain)
              attr                           = ExecutionAttributes.default.withPageSize(actual.size / 2)
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

  def actions: Spec[CQLExecutor, Throwable] =
    suite("Actions") {
      test("executeAction") {
        import ExecuteTestTable._
        // primary key is id
        val testGen = Gen.listOfN(10)(gen.toGenZIO).map(distinctBy(_.id))
        check(testGen) { elements =>
          val truncateData = truncate(table).execute.runDrain
          val toInsert     = elements.map(insert(table))
          val actual       = selectAllIn(table)(elements.map(_.id)).execute.runCollect.map(_.toList.sortBy(_.id))

          for {
            _       <- truncateData
            _       <- ZIO.foreachParDiscard(toInsert)(_.execute.runDrain)
            actual  <- actual
            expected = elements
          } yield assert(actual)(hasSameElements(expected))
        }
      } +
        test("executeBatchAction") {
          import ExecuteTestTable._
          // primary key is id
          val testGen = Gen.chunkOfN(10)(gen.toGenZIO).map(distinctBy(_.id))
          check(testGen) { elements =>
            val truncateData = truncate(batchTable).execute
            val batchedInsert: ZStream[CQLExecutor, Throwable, MutationResult] =
              elements
                .map(ExecuteTestTable.insert(batchTable))
                .reduce(_ + _)
                .batchType(BatchType.Unlogged)
                .execute

            val actual: ZStream[CQLExecutor, Throwable, ExecuteTestTable] =
              selectAllIn(batchTable)(elements.map(_.id)).execute

            for {
              _       <- truncateData.runDrain
              _       <- batchedInsert.runDrain
              actual  <- actual.runCollect.map(_.sortBy(_.id))
              expected = elements
            } yield assert(expected)(hasSameElements(actual))
          }
        } +
        test("executeMutation") {
          import ExecuteTestTable._
          check(gen.toGenZIO) { data =>
            val truncateData = truncate(table).executeMutation
            val toInsert     = insert(table)(data).executeMutation
            val search       = selectAllIn(table)(data.id :: Nil).execute.runCollect
            truncateData *> toInsert *> search.map(result => assert(result)(hasSameElements(List(data))))
          }
        }
    } @@ sequential

  def configuration: Spec[Sized & TestConfig & CqlSessionBuilder & CQLExecutor, Any] =
    suite("Session Configuration")(
      test("Creating a layer from an existing session allows you to access Cassandra") {
        val sessionScoped: URIO[CqlSessionBuilder & Scope, CqlSession] = {
          val createSession = for {
            builder <- ZIO.service[CqlSessionBuilder]
            session <- ZIO.attempt(builder.build())
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
        check(Gen.chunkOfN(4)(TimeoutCheckRow.gen.toGenZIO).map(distinctBy(_.id))) { rows =>
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
            .map(c => assertTrue(c == rows.size.toLong))
        }
      } @@ samples(1),
      test("PageSize are respected and matches with chunk size") {
        check(Gen.chunkOfN(4)(PageSizeCheckRow.gen.toGenZIO).map(distinctBy(_.id))) { rows =>
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
          ZStream.from(chunk) ++
            ZStream.paginateChunkZIO(page)(nextPage =>
              query
                .executePage(Some(nextPage))
                .map(r => (Chunk.fromIterable(r.data), r.pageState))
            )

        case Paged(chunk, None) =>
          ZStream.from(chunk)
      }

  // shim for Scala 2.12.x
  private def distinctBy[A, B](key: A => B)(in: Iterable[A]): Iterable[A] =
    in.groupBy(key).map { case (_, vs) => vs.head }
}
