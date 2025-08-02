package io.kaizensolutions.virgil
//
import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person
import io.kaizensolutions.virgil.models.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person._
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

class DeleteBuilderSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def maxParallelism: Int = 1

  override def checkConfig: CheckConfig =
    CheckConfig.default
      .copy(
        minimumSuccessful = 4,
        maximumGeneratorSize = 10,
        maximumDiscardRatio = 50,
        perPropertyParallelism = 1
      )

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("Delete the entire row") { executor =>
    forall(DeleteBuilderSpec_Person.gen) { person =>
      for {
        _            <- executor.executeMutation(truncate)
        _            <- executor.executeMutation(insert(person))
        afterInsert  <- executor.execute(find(person.id)).compile.toList.map(_.head)
        deleteResult <- executor.executeMutation(DeleteBuilder(tableName).entireRow.where(Id === person.id).build)
        afterDelete  <- executor.execute(find(person.id)).compile.toList.map(_.headOption)
      } yield expect.all(afterInsert == person, deleteResult.result, afterDelete.isEmpty)
    }
  }

  test("Delete columns in a row without deleting the entire row") { executor =>
    forall(DeleteBuilderSpec_Person.gen) { person =>
      for {
        _            <- executor.executeMutation(truncate)
        _            <- executor.executeMutation(insert(person))
        afterInsert  <- executor.execute(find(person.id)).compile.toList.map(_.headOption)
        deleteResult <- executor.executeMutation(
                          DeleteBuilder(tableName)
                            .columns(Name, Age)
                            .where(Id === person.id)
                            .build
                        )
        afterDelete <- executor.execute(find(person.id)).compile.toList.map(_.headOption)
      } yield expect.all(
        afterInsert.head == person,
        deleteResult.result,
        afterDelete.exists(_.name.isEmpty)
      )
    }
  }

  test("Conditional delete prevents a row that does not meet the criteria from being deleted") { executor =>
    forall(DeleteBuilderSpec_Person.gen) { person =>
      for {
        _ <- executor.executeMutation(truncate)
        _ <- executor.executeMutation(insert(person))
        _ <- executor.executeMutation(
               DeleteBuilder(tableName).entireRow
                 .where(Id === person.id)
                 .ifCondition(Age > person.age)
                 .build
             )
        after <- executor.execute(find(person.id)).take(1).compile.lastOrError
      } yield expect.all(after == person)
    }
  }
}
