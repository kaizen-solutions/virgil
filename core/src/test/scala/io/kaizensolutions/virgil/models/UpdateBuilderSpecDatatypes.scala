package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen

object UpdateBuilderSpecDatatypes {
  final case class UpdateBuilderSpecCounter(id: Int, likes: Long)
  object UpdateBuilderSpecCounter extends UpdateBuilderSpecCounterInstances {
    val gen: Gen[UpdateBuilderSpecCounter] =
      for {
        id    <- Gen.uuid.map(_.hashCode().abs.toInt)
        likes <- Gen.chooseNum(1L, 100L)
      } yield UpdateBuilderSpecCounter(id, likes)

    val tableName: String = "updatebuilderspec_counter"
    val Id                = "id"
    val Likes             = "likes"

    def find(id: Int) =
      SelectBuilder
        .from(tableName)
        .columns(Id, Likes)
        .where(Id === id)
        .build[UpdateBuilderSpecCounter]

    def insert(in: UpdateBuilderSpecCounter): CQL[MutationResult] =
      UpdateBuilder(tableName)
        .set(Likes += in.likes)
        .where(Id === in.id)
        .build

    def truncate: CQL[MutationResult] = CQL.truncate(tableName)
  }

  final case class UpdateBuilderSpecPerson(id: Int, name: String, age: Int)
  object UpdateBuilderSpecPerson extends UpdateBuilderSpecPersonInstances {
    val gen: Gen[UpdateBuilderSpecPerson] =
      for {
        id   <- Gen.uuid.map(_.hashCode.abs.toInt)
        name <- Gen.stringOf(Gen.alphaChar)
        age  <- Gen.chooseNum(18, 90)
      } yield UpdateBuilderSpecPerson(id, name, age)

    val tableName: String = "updatebuilderspec_person"
    val Id                = "id"
    val Name              = "name"
    val Age               = "age"

    def find(id: Int) =
      SelectBuilder
        .from(tableName)
        .columns(Id, Name, Age)
        .where(Id === id)
        .build[UpdateBuilderSpecPerson]

    def insert(in: UpdateBuilderSpecPerson): CQL[MutationResult] =
      InsertBuilder(tableName)
        .value("id", in.id)
        .value("name", in.name)
        .value("age", in.age)
        .build
  }
}
