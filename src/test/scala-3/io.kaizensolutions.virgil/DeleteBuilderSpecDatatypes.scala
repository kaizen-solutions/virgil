package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.dsl._
import zio.NonEmptyChunk

object DeleteBuilderSpecDatatypes {
  final case class DeleteBuilderSpec_Person(id: Int, name: Option[String], age: Option[Int])
  object DeleteBuilderSpec_Person {
    given cqlRowDecoderForDeleteBuilderSpec_Person: CqlRowDecoder.Object[DeleteBuilderSpec_Person] =
     CqlRowDecoder.derive[DeleteBuilderSpec_Person]

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
