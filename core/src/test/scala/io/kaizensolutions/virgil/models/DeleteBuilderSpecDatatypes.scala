package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.{CQL, MutationResult}
import zio.NonEmptyChunk

object DeleteBuilderSpecDatatypes {
  final case class DeleteBuilderSpec_Person(id: Int, name: Option[String], age: Option[Int])
  object DeleteBuilderSpec_Person extends DeleteBuilderSpec_PersonInstances {
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
  }
}
