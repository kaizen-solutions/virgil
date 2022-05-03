package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person
import io.kaizensolutions.virgil.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person._
import io.kaizensolutions.virgil.dsl._
import zio.test.TestAspect.{samples, sequential}
import zio.test._
import zio.{test => _, _}

object DeleteBuilderSpec {
  def deleteBuilderSpec =
    suite("Delete Builder Specification") {
      test("Delete the entire row") {
        check(deleteBuilderSpec_PersonGen) { person =>
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
      } + test("Delete columns in a row without deleting the entire row") {
        check(deleteBuilderSpec_PersonGen) { person =>
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
      } + test("Conditionally delete preventing a row from being deleted") {
        check(deleteBuilderSpec_PersonGen) { person =>
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
    id   <- Gen.int
    name <- Gen.string
    age  <- Gen.int
  } yield DeleteBuilderSpec_Person(id, Option(name), Option(age))
}
