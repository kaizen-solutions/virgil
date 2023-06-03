package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person
import io.kaizensolutions.virgil.models.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person._
import zio.test.TestAspect.samples
import zio.test.TestAspect.sequential
import zio.test._
import zio.test.scalacheck._

object DeleteBuilderSpec {
  def deleteBuilderSpec =
    suite("Delete Builder Specification") {
      test("Delete the entire row") {
        check(DeleteBuilderSpec_Person.gen.toGenZIO) { person =>
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
        check(DeleteBuilderSpec_Person.gen.toGenZIO) { person =>
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
        check(DeleteBuilderSpec_Person.gen.toGenZIO) { person =>
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
          } yield assertTrue(after.nonEmpty, after.head == person)
        }
      }
    } @@ sequential @@ samples(4)
}
