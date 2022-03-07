package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{GettableByName, UdtValue}

import scala.util.control.NoStackTrace

sealed trait DecoderException extends NoStackTrace { self =>
  def getCause: Throwable
  def getMessage: String
  def debug: String = self match {
    case s @ DecoderException.StructureReadFailure(message, field, _, cause) =>
      s"DecoderException.StructureReadFailure(message = $message, field = $field, structure = ${s.debugStructure}, cause = $cause)"

    case DecoderException.PrimitiveReadFailure(_, _) =>
      toString
  }

  override def toString: String = self match {
    case DecoderException.StructureReadFailure(message, field, structure, cause) =>
      s"DecoderException.StructureReadFailure(message = $message, field = $field, structure = $structure, cause = $cause, note = 'For more information call debugStructure or call debug'')"

    case DecoderException.PrimitiveReadFailure(message, cause) =>
      s"DecoderException.PrimitiveReadFailure(message = $message, cause = $cause)"
  }
}
object DecoderException {
  final case class StructureReadFailure(
    message: String,
    field: Option[FieldType],
    structure: GettableByName,
    cause: Throwable
  ) extends DecoderException {
    override def getCause: Throwable = cause
    override def getMessage: String  = message

    def debugStructure: String = structure match {
      case row: Row        => row.getFormattedContents
      case value: UdtValue => value.getFormattedContents
      case other           => other.toString
    }
  }

  final case class PrimitiveReadFailure(message: String, cause: Throwable) extends DecoderException {
    override def getCause: Throwable = cause
    override def getMessage: String  = message
  }

  sealed trait FieldType
  object FieldType {
    final case class Name(fieldName: String) extends FieldType
    final case class Index(fieldIndex: Int)  extends FieldType
  }
}
