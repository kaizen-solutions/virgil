package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person
import io.kaizensolutions.virgil.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person._
import io.kaizensolutions.virgil.dsl._
import zio.Has
import zio.random.Random
import zio.test.TestAspect.{samples, sequential}
import zio.test._

object DeleteBuilderSpec {
  def deleteBuilderSpec: Spec[Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[Any], TestSuccess] =
    suite("Delete Builder Specification") {
      testM("Delete the entire row") {
        checkM(deleteBuilderSpec_PersonGen) { person =>
          for {
            _            <- truncate.execute.runDrain
            _            <- insert(person).execute.runDrain
            afterInsert  <- find(person.id).execute.runHead
            deleteResult <- DeleteBuilder(tableName).entireRow.where(Id === person.id).build.execute.runHead.some
            afterDelete  <- find(person.id).execute.runHead
          } yield assertTrue(afterInsert.head == person) &&
            assertTrue(deleteResult.result) &&
            assertTrue(afterDelete.isEmpty)
        }
      } + testM("Delete columns in a row without deleting the entire row") {
        checkM(deleteBuilderSpec_PersonGen) { person =>
          for {
            _           <- truncate.execute.runDrain
            _           <- insert(person).execute.runDrain
            afterInsert <- find(person.id).execute.runHead
            deleteResult <- DeleteBuilder(tableName)
                              .columns(Name, Age)
                              .where(Id === person.id)
                              .build
                              .execute
                              .runHead
                              .some
            afterDelete <- find(person.id).execute.runHead.some
          } yield assertTrue(afterInsert.head == person) &&
            assertTrue(deleteResult.result) &&
            assertTrue(afterDelete == DeleteBuilderSpec_Person(id = person.id, name = None, age = None))
        }
      } + testM("Conditionally delete preventing a row from being deleted") {
        checkM(deleteBuilderSpec_PersonGen) { person =>
          for {
            _ <- truncate.execute.runDrain
            _ <- insert(person).execute.runDrain
            _ <- DeleteBuilder(tableName).entireRow
                   .where(Id === person.id)
                   .ifCondition(Age > person.age)
                   .build
                   .execute
                   .runDrain
            after <- find(person.id).execute.runHead
          } yield assertTrue(after.nonEmpty) && assertTrue(after.head == person)
        }
      }
    } @@ sequential @@ samples(4)

  def deleteBuilderSpec_PersonGen: Gen[Random with Sized, DeleteBuilderSpec_Person] = for {
    id   <- Gen.anyInt
    name <- Gen.anyString
    age  <- Gen.anyInt
  } yield DeleteBuilderSpec_Person(id, Option(name), Option(age))
}
