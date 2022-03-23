package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.DeleteBuilderSpec_Person._
import io.kaizensolutions.virgil.dsl._
import zio.random.Random
import zio.test.TestAspect.{samples, sequential}
import zio.test._
import zio.{Has, NonEmptyChunk}

object DeleteBuilderSpec {
  def deleteBuilderSpec: Spec[Has[CQLExecutor] with Random with Sized with TestConfig, TestFailure[Any], TestSuccess] =
    suite("Delete Builder Specification") {
      testM("Delete the entire row") {
        checkM(gen) { person =>
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
        checkM(gen) { person =>
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
        checkM(gen) { person =>
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
}

final case class DeleteBuilderSpec_Person(id: Int, name: Option[String], age: Option[Int])
object DeleteBuilderSpec_Person {
  val tableName                         = "deletebuilderspec_person"
  val Id                                = "id"
  val Name                              = "name"
  val Age                               = "age"
  val AllColumns: NonEmptyChunk[String] = NonEmptyChunk(Id, Name, Age)

  def find(id: Int): CQL[DeleteBuilderSpec_Person] =
    SelectBuilder
      .from(tableName)
      .columns(Id, Name, Age)
      .where(Id === id)
      .build[DeleteBuilderSpec_Person]

  def insert(in: DeleteBuilderSpec_Person): CQL[MutationResult] =
    InsertBuilder(tableName)
      .values(
        Id   -> in.id,
        Name -> in.name,
        Age  -> in.age
      )
      .ifNotExists
      .build

  val truncate: CQL[MutationResult] =
    CQL.truncate(tableName)

  def gen: Gen[Random with Sized, DeleteBuilderSpec_Person] = for {
    id   <- Gen.anyInt
    name <- Gen.anyString
    age  <- Gen.anyInt
  } yield DeleteBuilderSpec_Person(id, Option(name), Option(age))
}
