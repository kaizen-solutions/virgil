package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.dsl._

object InsertBuilderSpecDatatypes {
  final case class InsertBuilderSpecPerson(id: Int, name: String, age: Int)
  object InsertBuilderSpecPerson {
    val tableName = "insertbuilderspec_person"
    val Id        = "id"
    val Name      = "name"
    val Age       = "age"

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
}
