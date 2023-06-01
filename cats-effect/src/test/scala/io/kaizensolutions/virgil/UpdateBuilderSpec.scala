package io.kaizensolutions.virgil
//
import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.UpdateBuilderSpecDatatypes._
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

class UpdateBuilderSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def maxParallelism: Int = 1

  override def checkConfig: CheckConfig = CheckConfig.default.copy(minimumSuccessful = 20, perPropertyParallelism = 1)

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Performing an update will upsert a row") { executor =>
    import UpdateBuilderSpecPerson._

    forall(gen) { person =>
      val update =
        UpdateBuilder(tableName)
          .set(Name := person.name)
          .set(Age := person.age)
          .where(Id === person.id)
          .build

      for {
        _       <- executor.executeMutation(insert(person))
        _       <- executor.executeMutation(update)
        results <- executor.execute(find(person.id)).compile.toList
      } yield expect.all(results.contains(person), results.headOption.isDefined, results.head == person)
    }

  }

  test("Updating a column (using IF EXISTS) that does not exist will have no effect") { executor =>
    import UpdateBuilderSpecPerson._

    forall(gen) { person =>
      val updatedAge = person.age + 2
      val update =
        UpdateBuilder(tableName)
          .set(Age := updatedAge)
          .where(Id === person.id)
          .ifExists
          .build

      for {
        _          <- executor.executeMutation(DeleteBuilder(tableName).entireRow.where(Id === person.id).build)
        wasUpdated <- executor.executeMutation(update)
        results    <- executor.execute(find(person.id)).compile.toList
      } yield expect.all(results.isEmpty, !wasUpdated.result)
    }

  }

  test("Updating a counter column will correctly work") { executor =>
    import UpdateBuilderSpecCounter._

    forall(gen) { counter =>
      val update =
        UpdateBuilder(tableName)
          .set(Likes += 1L)
          .where(Id === counter.id)
          .build

      for {
        _          <- executor.executeMutation(truncate)
        _          <- executor.executeMutation(insert(counter))
        wasUpdated <- executor.executeMutation(update)
        results    <- executor.execute(find(counter.id)).compile.toList
      } yield expect.all(
        results.length == 1,
        results.head == counter.copy(likes = counter.likes + 1),
        wasUpdated.result
      )
    }

  }

  test("Updating a column using if conditions will update if met") { executor =>
    import UpdateBuilderSpecPerson._

    forall(gen) { person =>
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
        _          <- executor.executeMutation(insert(person))
        wasUpdated <- executor.executeMutation(update)
        results    <- executor.execute(find(person.id)).compile.toList
      } yield expect.all(results.contains(updatedPerson), wasUpdated.result)
    }
  }

}
