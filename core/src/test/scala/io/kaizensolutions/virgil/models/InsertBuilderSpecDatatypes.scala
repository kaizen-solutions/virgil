package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.dsl._
import org.scalacheck.Gen

object InsertBuilderSpecDatatypes {
  final case class InsertBuilderSpecPerson(id: Int, name: String, age: Int)
  object InsertBuilderSpecPerson extends InsertBuilderSpecPersonInstances {
    val gen: Gen[InsertBuilderSpecPerson] =
      for {
        id   <- Gen.chooseNum(1, 10000)
        name <- Gen.stringOfN(5, Gen.alphaChar)
        age  <- Gen.chooseNum(18, 80)
      } yield InsertBuilderSpecPerson(id, name, age)

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
  object WriteTimeNameResult extends WriteTimeNameResultInstances
}
