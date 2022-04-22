package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._

import java.time.{LocalDate, LocalTime}

object UserDefinedTypesSpecDatatypes {
  final case class Row_Person(
    id: Int,
    name: String,
    age: Int,
    data: UDT_Data
  )

  object Row_Person {
    def insert(person: Row_Person): CQL[MutationResult] =
      cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".mutation

    def select(id: Int): CQL[Row_Person] =
      cql"SELECT id, name, age, data FROM userdefinedtypesspec_person WHERE id = $id".query[Row_Person]
  }

  final case class UDT_Data(
    addresses: List[UDT_Address],
    email: Option[UDT_Email]
  )

  final case class UDT_Address(
    number: Int,
    street: String,
    city: String
  )

  final case class UDT_Email(
    username: String,
    @CqlColumn("domain_name") domainName: String,
    domain: String
  )

  final case class Row_HeavilyNestedUDTTable(
    id: Int,
    data: UDT_ExampleCollectionNestedUDTType
  )

  object Row_HeavilyNestedUDTTable {
    def insert(in: Row_HeavilyNestedUDTTable): CQL[MutationResult] =
      InsertBuilder("userdefinedtypesspec_heavilynestedudttable")
        .value("id", in.id)
        .value("data", in.data)
        .build

    def select(id: Int): CQL[Row_HeavilyNestedUDTTable] =
      SelectBuilder
        .from("userdefinedtypesspec_heavilynestedudttable")
        .column("id")
        .column("data")
        .where("id" === id)
        .build[Row_HeavilyNestedUDTTable]

  }

  final case class UDT_ExampleType(
    x: Long,
    y: Long,
    date: LocalDate,
    time: LocalTime
  )

  final case class UDT_ExampleNestedType(
    a: Int,
    b: String,
    c: UDT_ExampleType
  )

  final case class UDT_ExampleCollectionNestedUDTType(
    a: Int,
    b: Map[Int, Set[Set[Set[Set[UDT_ExampleNestedType]]]]],
    c: UDT_ExampleNestedType
  )
}
