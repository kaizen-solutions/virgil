package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.models.InsertBuilderSpecDatatypes._
import zio.test.TestAspect.samples
import zio.test.TestAspect.sequential
import zio.test._
import zio.test.scalacheck._
import zio.{test => _, _}

object InsertBuilderSpec {
  def insertBuilderSpec =
    suite("Insert Builder Specification") {
      test("Using TTL and exceeding it will cause the result to not be found") {
        check(InsertBuilderSpecPerson.gen.toGenZIO) { person =>
          val insert = InsertBuilderSpecPerson
            .insert(person)
            .usingTTL(1.second)
            .build
            .executeMutation

          val find =
            InsertBuilderSpecPerson
              .find(person.id)
              .execute
              .runHead

          insert *> Live.live(ZIO.sleep(1001.milliseconds)) *> find.map(res => assertTrue(res.isEmpty))
        }
      } + test("Using a timestamp is enforced") {
        check(InsertBuilderSpecPerson.gen.toGenZIO, Gen.long(13370000, 13371337)) { (person, timestamp) =>
          val insert = InsertBuilderSpecPerson
            .insert(person)
            .usingTimestamp(timestamp)
            .build
            .executeMutation

          val truncate = InsertBuilderSpecPerson.truncate.executeMutation

          val find =
            (s"""SELECT writetime(${InsertBuilderSpecPerson.Name})
               FROM ${InsertBuilderSpecPerson.tableName}
               WHERE ${InsertBuilderSpecPerson.Id} = """.asCql ++ cql"${person.id}")
              .query[WriteTimeNameResult]
              .execute
              .runHead
              .some

          truncate *> insert *> find.map(res => assertTrue(res.result == timestamp))
        }
      }
    } @@ sequential @@ samples(2)
}
