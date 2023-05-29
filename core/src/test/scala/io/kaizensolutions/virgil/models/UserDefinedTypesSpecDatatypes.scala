package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._

import java.time.LocalDate
import java.time.LocalTime

object UserDefinedTypesSpecDatatypes {
  final case class Row_Person(
    id: Int,
    name: String,
    age: Int,
    data: UDT_Data
  )

  object Row_Person extends Row_PersonInstances {
    def insert(person: Row_Person): CQL[MutationResult] =
      cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".mutation

    def select(id: Int): CQL[Row_Person] =
      cql"SELECT id, name, age, data FROM userdefinedtypesspec_person WHERE id = $id".query[Row_Person]
  }

  final case class UDT_Data(
    addresses: List[UDT_Address],
    email: Option[UDT_Email]
  )
  object UDT_Data extends UDT_DataInstances

  final case class UDT_Address(
    number: Int,
    street: String,
    city: String
  )
  object UDT_Address extends UDT_AddressInstances

  final case class UDT_Email(
    username: String,
    @CqlColumn("domain_name") domainName: String,
    domain: String
  )
  object UDT_Email extends UDT_EmailInstances

  final case class Row_HeavilyNestedUDTTable(
    id: Int,
    data: UDT_ExampleCollectionNestedUDTType
  )
  object Row_HeavilyNestedUDTTable extends Row_HeavilyNestedUDTTableInstances {
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
  object UDT_ExampleType extends UDT_ExampleTypeInstances

  final case class UDT_ExampleNestedType(
    a: Int,
    b: String,
    c: UDT_ExampleType
  )
  object UDT_ExampleNestedType extends UDT_ExampleNestedTypeInstances

  final case class UDT_ExampleCollectionNestedUDTType(
    a: Int,
    b: Map[Int, Set[Set[Set[Set[UDT_ExampleNestedType]]]]],
    c: UDT_ExampleNestedType
  )
  object UDT_ExampleCollectionNestedUDTType extends UDT_ExampleCollectionNestedUDTTypeInstances
}
