package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.annotations.CqlColumn
import io.kaizensolutions.virgil.codecs.*
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
    given cqlRowDecoderForRow_Person: CqlRowDecoder.Object[Row_Person] = CqlRowDecoder.derive[Row_Person]

    def insert(person: Row_Person): CQL[MutationResult] =
      cql"INSERT INTO userdefinedtypesspec_person (id, name, age, data) VALUES (${person.id}, ${person.name}, ${person.age}, ${person.data})".mutation

    def select(id: Int): CQL[Row_Person] =
      cql"SELECT id, name, age, data FROM userdefinedtypesspec_person WHERE id = $id".query[Row_Person]
  }

  final case class UDT_Data(
    addresses: List[UDT_Address],
    email: Option[UDT_Email]
  )
  object UDT_Data {
    given cqlUdtValueEncoderForUDT_Data: CqlUdtValueEncoder.Object[UDT_Data] = CqlUdtValueEncoder.derive[UDT_Data]
    given cqlUdtValueDecoderForUDT_Data: CqlUdtValueDecoder.Object[UDT_Data] = CqlUdtValueDecoder.derive[UDT_Data]
  }

  final case class UDT_Address(
    number: Int,
    street: String,
    city: String
  )
  object UDT_Address {
    given cqlUdtValueEncoderForUDT_Address: CqlUdtValueEncoder.Object[UDT_Address] =
      CqlUdtValueEncoder.derive[UDT_Address]
    given cqlUdtValueDecoderForUDT_Address: CqlUdtValueDecoder.Object[UDT_Address] =
      CqlUdtValueDecoder.derive[UDT_Address]
  }

  final case class UDT_Email(
    username: String,
    @CqlColumn("domain_name") domainName: String,
    domain: String
  )
  object UDT_Email {
    given cqlUdtValueEncoderForUDT_Email: CqlUdtValueEncoder.Object[UDT_Email] = CqlUdtValueEncoder.derive[UDT_Email]
    given cqlUdtValueDecoderForUDT_Email: CqlUdtValueDecoder.Object[UDT_Email] = CqlUdtValueDecoder.derive[UDT_Email]
  }

  final case class Row_HeavilyNestedUDTTable(
    id: Int,
    data: UDT_ExampleCollectionNestedUDTType
  )
  object Row_HeavilyNestedUDTTable {
    given cqlRowDecoderForRow_HeavilyNestedUDTTable: CqlRowDecoder.Object[Row_HeavilyNestedUDTTable] =
      CqlRowDecoder.derive[Row_HeavilyNestedUDTTable]

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
  object UDT_ExampleType {
    given cqlUdtValueEncoderForUDT_ExampleType: CqlUdtValueEncoder.Object[UDT_ExampleType] =
      CqlUdtValueEncoder.derive[UDT_ExampleType]

    given cqlUdtValueDecoderForUDT_ExampleType: CqlUdtValueDecoder.Object[UDT_ExampleType] =
      CqlUdtValueDecoder.derive[UDT_ExampleType]
  }

  final case class UDT_ExampleNestedType(
    a: Int,
    b: String,
    c: UDT_ExampleType
  )
  object UDT_ExampleNestedType {
    given cqlUdtValueEncoderForUDT_ExampleNestedType: CqlUdtValueEncoder.Object[UDT_ExampleNestedType] =
      CqlUdtValueEncoder.derive[UDT_ExampleNestedType]

    given cqlUdtValueDecoderForUDT_ExampleNestedType: CqlUdtValueDecoder.Object[UDT_ExampleNestedType] =
      CqlUdtValueDecoder.derive[UDT_ExampleNestedType]
  }

  final case class UDT_ExampleCollectionNestedUDTType(
    a: Int,
    b: Map[Int, Set[Set[Set[Set[UDT_ExampleNestedType]]]]],
    c: UDT_ExampleNestedType
  )
  object UDT_ExampleCollectionNestedUDTType {
    given cqlUdtValueEncoderForUDT_ExampleCollectionNestedUDTType
      : CqlUdtValueEncoder.Object[UDT_ExampleCollectionNestedUDTType] =
      CqlUdtValueEncoder.derive[UDT_ExampleCollectionNestedUDTType]

    given cqlUdtValueDecoderForUDT_ExampleCollectionNestedUDTType
      : CqlUdtValueDecoder.Object[UDT_ExampleCollectionNestedUDTType] =
      CqlUdtValueDecoder.derive[UDT_ExampleCollectionNestedUDTType]
  }
}
