package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.UpdateBuilderSpecDatatypes._
import zio.test.TestAspect._
import zio.test._

object UpdateBuilderSpec {
  def updateBuilderSpec =
    suite("UpdateBuilder Specification") {
      test("Performing an update will upsert a row") {
        import UpdateBuilderSpecPerson._
        check(updateBuilderSpecPersonGen) { person =>
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
        test("Updating a column (using IF EXISTS) that does not exist will have no effect") {
          import UpdateBuilderSpecPerson._
          check(updateBuilderSpecPersonGen) { person =>
            val updatedAge = person.age + 2
            val update =
              UpdateBuilder(tableName)
                .set(Age := updatedAge)
                .where(Id === person.id)
                .ifExists
                .build

            for {
              _          <- DeleteBuilder(tableName).entireRow.where(Id === person.id).build.executeMutation
              wasUpdated <- update.executeMutation
              results    <- find(person.id).execute.runHead
            } yield assertTrue(results.isEmpty) && assertTrue(!wasUpdated.result)
          }
        } +
        test("Updating a counter column will correctly work") {
          import UpdateBuilderSpecCounter._
          check(updateBuilderSpecCounterGen) { counter =>
            val update =
              UpdateBuilder(tableName)
                .set(Likes += 1L)
                .where(Id === counter.id)
                .build

            for {
              _          <- insert(counter).executeMutation
              wasUpdated <- update.executeMutation
              results    <- find(counter.id).execute.runCollect
            } yield assertTrue(
              results.length == 1,
              results.head == counter.copy(likes = counter.likes + 1),
              wasUpdated.result
            )
          }
        } +
        test("Updating a column using if conditions will update if met") {
          import UpdateBuilderSpecPerson._
          check(updateBuilderSpecPersonGen) { person =>
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

  val updateBuilderSpecPersonGen: Gen[Sized, UpdateBuilderSpecPerson] = for {
    id   <- Gen.int(1, 10000)
    name <- Gen.string(Gen.alphaChar)
    age  <- Gen.int(18, 90)
  } yield UpdateBuilderSpecPerson(id, name, age)

  val updateBuilderSpecCounterGen: Gen[Any, UpdateBuilderSpecCounter] =
    for {
      id    <- Gen.int(1, 10000)
      likes <- Gen.long(1, 100)
    } yield UpdateBuilderSpecCounter(id, likes)
}
