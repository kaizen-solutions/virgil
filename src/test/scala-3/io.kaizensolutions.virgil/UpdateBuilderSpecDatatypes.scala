package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.dsl._

object UpdateBuilderSpecDatatypes {
  final case class UpdateBuilderSpecCounter(id: Int, likes: Long)
  object UpdateBuilderSpecCounter {
    given cqlRowDecoder: CqlRowDecoder.Object[UpdateBuilderSpecCounter] =
      CqlRowDecoder.derive[UpdateBuilderSpecCounter]

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
  }

  final case class UpdateBuilderSpecPerson(id: Int, name: String, age: Int)
  object UpdateBuilderSpecPerson {
    given cqlRowDecoderForUpdateBuilderSpecPerson: CqlRowDecoder.Object[UpdateBuilderSpecPerson] =
      CqlRowDecoder.derive[UpdateBuilderSpecPerson]

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
