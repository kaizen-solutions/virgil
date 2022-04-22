package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.RelationSpecDatatypes._
import RelationSpec_Person._
import io.kaizensolutions.virgil.dsl._
import zio.Has
import zio.random.Random
import zio.test.TestAspect.{samples, sequential}
import zio.test._

object RelationSpec {
  def relationSpec: ZSpec[TestConfig with Random with Has[CQLExecutor], Any] =
    suite("Relational Operators Specification") {
      testM("isNull") {
        checkM(relationSpec_PersonGen) { person =>
          val update =
            UpdateBuilder(table)
              .set(Name := person.name)
              .set(Age := person.age)
              .where(Id === person.id)
              .ifCondition(Name.isNull)
              .build
              .execute
              .runDrain

          val find = RelationSpec_Person.find(person.id).execute.runHead.some

          truncate.execute.runDrain *>
            update *>
            find.map(actual => assertTrue(actual == person))
        }
      } + testM("isNotNull") {
        checkM(relationSpec_PersonGen) { person =>
          val insert  = RelationSpec_Person.insert(person).execute.runDrain
          val newName = person.name + " " + person.name
          val update =
            UpdateBuilder(RelationSpec_Person.table)
              .set(Name := newName)
              .where(Id === person.id)
              .ifCondition(Name.isNotNull)
              .build
              .execute
              .runDrain

          val find = RelationSpec_Person.find(person.id).execute.runHead.some

          truncate.execute.runDrain *>
            insert *>
            update *>
            find.map(actual => assertTrue(actual == person.copy(name = newName)))
        }
      }
    } @@ sequential @@ samples(4)

  def relationSpec_PersonGen: Gen[Random, RelationSpec_Person] =
    for {
      id   <- Gen.int(1, 1000)
      name <- Gen.stringBounded(2, 4)(Gen.alphaChar)
      age  <- Gen.int(1, 100)
    } yield RelationSpec_Person(id, name, age)
}
