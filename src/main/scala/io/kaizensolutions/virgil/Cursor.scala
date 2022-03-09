package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{GettableByName, UdtValue}
import io.kaizensolutions.virgil.codecs._
import zio.Chunk

import scala.util.control.NonFatal

final case class RowCursor(
  private val history: Chunk[String],
  private val current: Row
) {
  def downUdtValue(name: String): Either[DecoderException, UdtValueCursor] =
    Cursor.downUdtValue(name, current, history)

  def field[A](name: String)(implicit ev: CqlPrimitiveDecoder[A]): Either[DecoderException, A] =
    Cursor.field(current, name, history)(ev)

  def viewAs[A](implicit ev: codecs.CqlRowDecoder.Object[A]): Either[DecoderException, A] =
    ev.either
      .decode(current)
      .left
      .map(Cursor.enrichError(history))
}
object RowCursor {
  def apply(row: Row): RowCursor = RowCursor(Chunk.empty, row)
}

final case class UdtValueCursor(private val history: Chunk[String], private val current: UdtValue) {
  def downUdtValue(name: String): Either[DecoderException, UdtValueCursor] = Cursor.downUdtValue(name, current, history)

  def field[A](name: String)(implicit ev: CqlPrimitiveDecoder[A]): Either[DecoderException, A] =
    Cursor.field(current, name, history)(ev)

  def viewAs[A](implicit ev: CqlUdtValueDecoder.Object[A]): Either[DecoderException, A] =
    ev.either
      .decode(current)
      .left
      .map(Cursor.enrichError(history))
}
object UdtValueCursor {
  def apply(udtValue: UdtValue): UdtValueCursor = UdtValueCursor(Chunk.empty, udtValue)
}

private object Cursor {
  def downUdtValue(
    fieldName: String,
    current: GettableByName,
    history: Chunk[String]
  ): Either[DecoderException, UdtValueCursor] =
    try (Right(UdtValueCursor(history :+ fieldName, current.getUdtValue(fieldName))))
    catch {
      case NonFatal(cause) =>
        Left(
          DecoderException.StructureReadFailure(
            s"Failed to get UdtValue from Row ${Cursor.renderHistory(history)}",
            Some(DecoderException.FieldType.Name(fieldName)),
            current,
            cause
          )
        )
    }

  def field[A](current: GettableByName, fieldName: String, history: Chunk[String])(implicit
    ev: CqlPrimitiveDecoder[A]
  ): Either[DecoderException, A] =
    try (Right(CqlPrimitiveDecoder.decodePrimitiveByFieldName(current, fieldName)(ev)))
    catch {
      case NonFatal(d: DecoderException) =>
        Left(d)

      case NonFatal(cause) =>
        Left(
          DecoderException.PrimitiveReadFailure(s"Failed to read field $fieldName. ${renderHistory(history)}", cause)
        )
    }

  def renderHistory(history: Chunk[String]): String =
    history.mkString(start = "History(", sep = " -> ", end = ")")

  def enrichError(history: Chunk[String])(decoderException: DecoderException): DecoderException = {
    val historyRendered = renderHistory(history)
    decoderException match {
      case s @ DecoderException.StructureReadFailure(message, _, _, _) =>
        s.copy(message = s"$message. $historyRendered")

      case p @ DecoderException.PrimitiveReadFailure(message, _) =>
        p.copy(message = s"$message. $historyRendered")
    }
  }
}
