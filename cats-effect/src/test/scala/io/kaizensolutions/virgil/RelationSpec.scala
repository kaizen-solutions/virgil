package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.RelationSpecDatatypes.RelationSpec_Person._
import io.kaizensolutions.virgil.models.RelationSpecDatatypes._
import weaver._
import weaver.scalacheck.CheckConfig
import weaver.scalacheck.Checkers

class RelationSpec(global: GlobalRead) extends IOSuite with ResourceSuite with Checkers {

  override def maxParallelism: Int = 1

  override def checkConfig: CheckConfig = CheckConfig.default.copy(
    minimumSuccessful = 4,
    maximumGeneratorSize = 10,
    maximumDiscardRatio = 50,
    perPropertyParallelism = 1
  )

  override type Res = CQLExecutor[IO]

  override def sharedResource: Resource[IO, Res] = global.getOrFailR[Res]()

  test("isNull") { executor =>
    forall(RelationSpec_Person.gen) { expected =>
      for {
        _ <- executor.executeMutation(RelationSpec_Person.insert(expected))
        _ <- executor
               .executeMutation(
                 UpdateBuilder(table)
                   .set(Name := expected.name)
                   .set(Age := expected.age)
                   .where(Id === expected.id)
                   .ifCondition(Name.isNull)
                   .build
               )
        find <- executor.execute(RelationSpec_Person.find(expected.id)).compile.toList.map(_.headOption)
        _    <- executor.executeMutation(RelationSpec_Person.truncate)
      } yield expect.all(find.contains(expected))
    }
  }

  test("isNotNull") { executor =>
    forall(RelationSpec_Person.gen) { expected =>
      val newName = expected.name + " " + expected.name
      for {
        _ <- executor.executeMutation(RelationSpec_Person.insert(expected))
        _ <- executor
               .executeMutation(
                 UpdateBuilder(table)
                   .set(Name := newName)
                   .where(Id === expected.id)
                   .ifCondition(Name.isNotNull)
                   .build
               )
        find <- executor.execute(RelationSpec_Person.find(expected.id)).compile.toList.map(_.headOption)
        _    <- executor.executeMutation(RelationSpec_Person.truncate)
      } yield expect.all(find.exists(_ == expected.copy(name = newName)))
    }
  }
}
