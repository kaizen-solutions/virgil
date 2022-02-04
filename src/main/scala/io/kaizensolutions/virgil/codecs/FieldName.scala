package io.kaizensolutions.virgil.codecs

/**
 * This type is used by Readers and Writers to decide whether to utilize schema
 * data when reading and writing data from the Datastax DataType
 */
sealed trait FieldName
object FieldName {
  case object Unused                       extends FieldName
  final case class Labelled(value: String) extends FieldName
}
