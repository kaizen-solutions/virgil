package io.kaizensolutions.virgil.codecs

import scala.deriving.Mirror
import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

// Note: Fully automatic derivation is not yet present in Scala 3 just yet (because I haven't figured out how to do it yet)
trait UdtValueEncoderMagnoliaDerivation extends ProductDerivation[CqlUdtValueEncoder]:
  final def join[T](ctx: CaseClass[Typeclass,T]): CqlUdtValueEncoder.Object[T] =
    (structure: UdtValue, value: T) =>
      ctx.params.foldLeft(structure) { case (acc, param) =>
        val fieldName  = CqlColumn.extractFieldName(param.annotations).getOrElse(param.label)
        val fieldValue = param.deref(value)
        val encoder    = param.typeclass
        encoder.encodeByFieldName(acc, fieldName, fieldValue)
      }

  inline def derive[A](using Mirror.Of[A]): CqlUdtValueEncoder.Object[A] =
    derived[A].asInstanceOf[CqlUdtValueEncoder.Object[A]]
