package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.models.InsertBuilderSpecDatatypes._
import org.scalacheck.Gen
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

import java.time.{Duration => JDuration}
import scala.concurrent.duration._

class InsertBuilderSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def checkConfig: CheckConfig = CheckConfig.default.copy(minimumSuccessful = 20, perPropertyParallelism = 1)

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Using TTL and exceeding it will cause the result to not be found") { executor =>
    forall(InsertBuilderSpecPerson.gen) { person =>
      for {
        _    <- executor.executeMutation(InsertBuilderSpecPerson.insert(person).usingTTL(JDuration.ofSeconds(1)).build)
        _    <- IO.sleep(1001.milliseconds)
        find <- executor
                  .execute(InsertBuilderSpecPerson.find(person.id))
                  .compile
                  .toList
                  .map(_.headOption)
      } yield expect.all(find.isEmpty)

    }
  }
  test("Using a timestamp is enforced") { executor =>
    val gen = for {
      person    <- InsertBuilderSpecPerson.gen
      timestamp <- Gen.chooseNum(13370000, 13371337)
    } yield (person, timestamp)

    forall(gen) { case (person, timestamp) =>
      val query =
        (s"""SELECT writetime(${InsertBuilderSpecPerson.Name})
               FROM ${InsertBuilderSpecPerson.tableName}
               WHERE ${InsertBuilderSpecPerson.Id} = """.asCql ++ cql"${person.id}")
          .query[WriteTimeNameResult]

      for {
        _      <- executor.executeMutation(InsertBuilderSpecPerson.insert(person).usingTimestamp(timestamp.toLong).build)
        actual <- executor.execute(query).take(1).compile.lastOrError
        _      <- executor.executeMutation(InsertBuilderSpecPerson.truncate)
      } yield expect.all(actual.result == timestamp)
    }
  }
}
