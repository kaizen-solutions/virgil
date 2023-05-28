package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.{CQL, MutationResult}

object RelationSpecDatatypes {
  final case class RelationSpec_Person(
    id: Int,
    name: String,
    age: Int
  )
  object RelationSpec_Person extends RelationSpec_PersonInstances {
    val Id   = "id"
    val Name = "name"
    val Age  = "age"

    val table: String = "relationspec_person"

    val truncate: CQL[MutationResult] = s"TRUNCATE TABLE $table".asCql.mutation

    def insert(in: RelationSpec_Person): CQL[MutationResult] =
      InsertBuilder(table)
        .value(Id, in.id)
        .value(Name, in.name)
        .value(Age, in.age)
        .build

    def find(id: Int): CQL[RelationSpec_Person] =
      SelectBuilder
        .from(table)
        .columns(Id, Name, Age)
        .where(Id === id)
        .build[RelationSpec_Person]
  }
}
