package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.RelationSpecDatatypes.RelationSpec_Person._
import io.kaizensolutions.virgil.models.RelationSpecDatatypes._
import zio.test.TestAspect.samples
import zio.test.TestAspect.sequential
import zio.test._
import zio.test.scalacheck._

object RelationSpec {
  def relationSpec =
    suite("Relational Operators Specification") {
      test("isNull") {
        check(RelationSpec_Person.gen.toGenZIO) { person =>
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
      } + test("isNotNull") {
        check(RelationSpec_Person.gen.toGenZIO) { person =>
          val insert  = RelationSpec_Person.insert(person).execute.runDrain
          val newName = person.name + " " + person.name
          val update  =
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
}
