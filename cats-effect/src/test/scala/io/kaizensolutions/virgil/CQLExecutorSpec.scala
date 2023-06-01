package io.kaizensolutions.virgil

import cats.effect._
import cats.syntax.all._
import fs2.{Chunk, Stream}
import io.kaizensolutions.virgil.configuration.{ConsistencyLevel, ExecutionAttributes}
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes._
import org.scalacheck.Gen
import weaver.scalacheck.{CheckConfig, Checkers}
import weaver.{GlobalRead, IOSuite}

import scala.concurrent.duration._

class CQLExecutorSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def maxParallelism: Int = 1
  override def checkConfig: CheckConfig = CheckConfig.default.copy(
    minimumSuccessful = 4,
    maximumGeneratorSize = 10,
    maximumDiscardRatio = 50,
    perPropertyParallelism = 1
  )

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("selectFirst") { executor =>
    executor
      .execute(
        cql"SELECT now() FROM system.local"
          .query[SystemLocalResponse]
          .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
      )
      .compile
      .last
      .map(result => expect(result.flatMap(_.time.toOption).get > 0L))
  }

  test("select") { executor =>
    executor
      .execute(
        cql"SELECT prepared_id, logged_keyspace, query_string FROM system.prepared_statements"
          .query[PreparedStatementsResponse]
          .withAttributes(ExecutionAttributes.default.withConsistencyLevel(ConsistencyLevel.LocalOne))
      )
      .compile
      .toList
      .map(results =>
        expect(results.forall { r =>
          import r._
          query.contains("SELECT") ||
          query.contains("UPDATE") ||
          query.contains("CREATE") ||
          query.contains("DELETE") ||
          query.contains("INSERT") ||
          query.contains("USE")
        })
      )
  }

  test("selectPage") { executor =>
    import SelectPageRow._
    // id is the primary key
    val testGen = Gen.listOfN(50, gen).map(distinctBy(_.id)).map(_.toList)
    forall(testGen) { actual =>
      for {
        _                             <- executor.executeMutation(truncate)
        _                             <- Stream.iterable(actual).parEvalMapUnbounded(row => executor.executeMutation(insert(row))).compile.drain
        attr                           = ExecutionAttributes.default.withPageSize(actual.size / 2)
        all                            = executor.execute(selectAll.withAttributes(attr)).compile.toList
        paged                          = selectPageStream[SelectPageRow](executor, selectAll.withAttributes(attr)).compile.toList
        result                        <- (all, paged).parMapN((a, p) => (a, p))
        (dataFromSelect, dataFromPage) = result
      } yield {
        expect.same(dataFromPage.length, dataFromSelect.length) &&
        expect.same(dataFromSelect.length, actual.length) &&
        expect.same(dataFromPage.toSet, dataFromSelect.toSet) &&
        expect.same(dataFromSelect.toSet, actual.toSet)
      }
    }
  }

  test("take") { executor =>
    forall(Gen.chooseNum(2L, 1000L)) { n =>
      executor
        .execute(cql"SELECT * FROM system.local".query.take(n))
        .compile
        .count
        .map(rowCount => expect.all(rowCount > 0L, rowCount <= n))
    }
  }

  test("actions") { executor =>
    import ExecuteTestTable._
    // primary key is id
    val testGen = Gen.listOfN(10, gen).map(distinctBy(_.id)).map(_.toList)
    forall(testGen) { elements =>
      val truncateData = executor.execute(truncate(table)).compile.drain
      val toInsert     = elements.map(insert(table)).toList
      val actual       = executor.execute(selectAllIn(table)(elements.map(_.id))).compile.toList

      for {
        _       <- truncateData
        _       <- toInsert.traverse_(executor.executeMutation)
        actual  <- actual
        expected = elements
      } yield expect.same(actual.toSet, expected.toSet) && expect.same(actual.length, expected.length)
    }
  }

  test("batch") { executor =>
    import ExecuteTestTable._
    // primary key is id
    val testGen = Gen.listOfN(10, gen).map(distinctBy(_.id)).map(_.toList)
    forall(testGen) { elements =>
      val truncateData = executor.execute(truncate(batchTable))
      val batchedInsert: Stream[IO, MutationResult] =
        executor.execute(
          elements
            .map(ExecuteTestTable.insert(batchTable))
            .reduce(_ + _)
            .batchType(BatchType.Unlogged)
        )

      val actual: Stream[IO, ExecuteTestTable] =
        executor.execute(selectAllIn(batchTable)(elements.map(_.id)))
      (
        truncateData.drain ++
          batchedInsert.drain ++
          actual
      ).compile.toList.map { actual =>
        val expected = elements
        expect.same(actual.toSet, expected.toSet) &&
        expect.same(actual.length, expected.length)
      }
    }
  }

  test("executeMutation") { executor =>
    import ExecuteTestTable._
    forall(gen) { data =>
      val truncateData = executor.executeMutation(truncate(table))
      val toInsert     = executor.executeMutation(insert(table)(data))
      val search       = executor.execute(selectAllIn(table)(data.id :: Nil)).compile.toList
      truncateData >> toInsert >> search.map { result =>
        expect.same(result, List(data))
      }
    }
  }

  test("Timeouts are respected") { executor =>
    val testGen = Gen.listOfN(4, TimeoutCheckRow.gen).map(distinctBy(_.id)).map(_.toList)
    val rows    = testGen.retryUntil(_.nonEmpty, 10).sample.get
    val insert =
      Stream
        .iterable(rows)
        .map(TimeoutCheckRow.insert)
        .covary[IO]
        .timeout(4.seconds)
        .flatMap(executor.execute)

    val select =
      executor
        .execute(TimeoutCheckRow.selectAll)
        .timeout(2.seconds)
        .compile
        .count

    (insert.compile.drain *> select)
      .map(c => expect.same(c, rows.length.toLong))
  }

  test("page size is respected and matches with chunk size") { executor =>
    val testGen = Gen.listOfN(10, PageSizeCheckRow.gen).map(distinctBy(_.id)).map(_.toList)
    // get a list of 4 elements
    val rows = testGen.retryUntil(_.size >= 4, 10).sample.get.take(4)

    val insert =
      Stream
        .iterable(rows)
        .covary[IO]
        .map(PageSizeCheckRow.insert)
        .map(_.timeout(javaDuration(4.seconds)))
        .flatMap(executor.execute)

    val select =
      executor
        .execute(
          PageSizeCheckRow.selectAll
            .pageSize(2)
            .timeout(javaDuration(2.second))
        )
        .mapChunks(s => Chunk.singleton(s.size))
        .compile
        .toList

    (insert.compile.drain >> select)
      .map(c => expect.all(c.size == 2, c.forall(_ == 2)))
  }

  // Used to provide a similar API as the `select` method
  private def selectPageStream[ScalaType](
    executor: CQLExecutor[IO],
    query: CQL[ScalaType]
  ): Stream[IO, ScalaType] =
    Stream.eval(executor.executePage(query, None)).flatMap {
      case Paged(chunk, Some(page)) =>
        Stream.chunk(chunk) ++
          Stream
            .unfoldChunkEval(Option(page)) {
              case Some(page) =>
                executor.executePage(query, Some(page)).flatMap {
                  case Paged(chunk, Some(nextPage)) => IO.pure(Option((chunk, nextPage.some)))
                  case Paged(chunk, None)           => IO.pure(Option((chunk, none)))
                }

              case None =>
                IO.pure(None)
            }

      case Paged(chunk, None) =>
        Stream.chunk(chunk)
    }

  private def distinctBy[A, B](key: A => B)(in: Iterable[A]): Iterable[A] =
    in.groupBy(key).map { case (_, vs) => vs.head }

  private def javaDuration(duration: FiniteDuration): java.time.Duration =
    java.time.Duration.ofNanos(duration.toNanos)
}
