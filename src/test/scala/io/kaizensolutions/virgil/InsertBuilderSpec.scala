package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
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
        checkM(InsertBuilderSpecPerson.gen) { person =>
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
        checkM(InsertBuilderSpecPerson.gen, Gen.long(13370000, 13371337)) { (person, timestamp) =>
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
final case class InsertBuilderSpecPerson(id: Int, name: String, age: Int)
object InsertBuilderSpecPerson {
  val tableName = "insertbuilderspec_person"
  val Id        = "id"
  val Name      = "name"
  val Age       = "age"

  def gen: Gen[Random, InsertBuilderSpecPerson] =
    for {
      id   <- Gen.int(1, 10000)
      name <- Gen.stringN(5)(Gen.alphaChar)
      age  <- Gen.int(18, 80)
    } yield InsertBuilderSpecPerson(id, name, age)

  def truncate: CQL[MutationResult] = CQL.truncate(tableName)

  def insert(in: InsertBuilderSpecPerson): InsertBuilder[InsertState.ColumnAdded] =
    InsertBuilder(tableName)
      .values(
        Id   -> in.id,
        Name -> in.name,
        Age  -> in.age
      )

  def find(id: Int): CQL[InsertBuilderSpecPerson] =
    SelectBuilder
      .from(tableName)
      .allColumns
      .where(Id === id)
      .build[InsertBuilderSpecPerson]
}

final case class WriteTimeNameResult(@CqlColumn("writetime(name)") result: Long)
