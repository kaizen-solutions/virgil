package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.GettableByName

import scala.util.control.NoStackTrace

final case class DecoderException(
  message: String,
  field: FieldType,
  structure: GettableByName,
  cause: Throwable
) extends Exception(message, cause)
    with NoStackTrace

sealed trait FieldType
object FieldType {
  final case class Name(fieldName: String) extends FieldType
  final case class Index(fieldIndex: Int)  extends FieldType
}
