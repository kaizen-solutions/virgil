package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.UpdateBuilderSpecDatatypes.UpdateBuilderSpecPerson
import io.kaizensolutions.virgil.dsl._
import zio.random.Random
import zio.test.TestAspect._
import zio.test._

object UpdateBuilderSpec {
  def updateBuilderSpec =
    suite("UpdateBuilder Specification") {
      testM("Performing an update will upsert a row") {
        import UpdateBuilderSpecPerson._
        checkM(updateBuilderSpecPersonGen) { person =>
          val update =
            UpdateBuilder(tableName)
              .set(Name := person.name)
              .set(Age := person.age)
              .where(Id === person.id)
              .build

          update.execute.runDrain *> find(person.id).execute.runHead.map(result =>
            assertTrue(result.isDefined) && assertTrue(result.get == person)
          )
        }
      } +
        testM("Updating a column (using IF EXISTS) that does not exist will have no effect") {
          import UpdateBuilderSpecPerson._
          checkM(updateBuilderSpecPersonGen) { person =>
            val updatedAge = person.age + 2
            val update =
              UpdateBuilder(tableName)
                .set(Age := updatedAge)
                .where(Id === person.id)
                .ifExists
                .build

            for {
              wasUpdated <- update.execute.runHead.some
              results    <- find(person.id).execute.runHead
            } yield assertTrue(results.isEmpty) && assertTrue(!wasUpdated.result)
          }
        } +
        testM("Updating a column using if conditions will update if met") {
          import UpdateBuilderSpecPerson._
          checkM(updateBuilderSpecPersonGen) { person =>
            val updatedAge = person.age + 10
            val update =
              UpdateBuilder(tableName)
                .set(Age := updatedAge)
                .where(Id === person.id)
                .ifCondition(Age <= person.age)
                .andIfCondition(Name === person.name)
                .build

            val updatedPerson = person.copy(age = updatedAge)
            for {
              _          <- insert(person).execute.runDrain
              wasUpdated <- update.execute.runHead.some
              results    <- find(person.id).execute.runHead
            } yield assertTrue(results.contains(updatedPerson)) && assertTrue(wasUpdated.result)
          }
        }
    } @@ samples(4) @@ sequential @@ nondeterministic

  def updateBuilderSpecPersonGen: Gen[Random with Sized, UpdateBuilderSpecPerson] = for {
    id   <- Gen.int(1, 10000)
    name <- Gen.string(Gen.alphaChar)
    age  <- Gen.int(18, 90)
  } yield UpdateBuilderSpecPerson(id, name, age)
}
