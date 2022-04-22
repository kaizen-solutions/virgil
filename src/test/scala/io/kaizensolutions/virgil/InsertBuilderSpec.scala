package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.InsertBuilderSpecDatatypes._
import io.kaizensolutions.virgil.cql._
import zio.ZIO
import zio.duration._
import zio.random.Random
import zio.test.TestAspect.{samples, sequential}
import zio.test._
import zio.test.environment.Live

object InsertBuilderSpec {
  def insertBuilderSpec =
    suite("Insert Builder Specification") {
      testM("Using TTL and exceeding it will cause the result to not be found") {
        checkM(insertBuilderSpecPersonGen) { person =>
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
      } + testM("Using a timestamp is enforced") {
        checkM(insertBuilderSpecPersonGen, Gen.long(13370000, 13371337)) { (person, timestamp) =>
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

  def insertBuilderSpecPersonGen: Gen[Random, InsertBuilderSpecPerson] =
    for {
      id   <- Gen.int(1, 10000)
      name <- Gen.stringN(5)(Gen.alphaChar)
      age  <- Gen.int(18, 80)
    } yield InsertBuilderSpecPerson(id, name, age)
}
