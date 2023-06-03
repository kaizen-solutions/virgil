package io.kaizensolutions.virgil.codecs

import scala.deriving.Mirror
import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1.*

// Note: Fully automatic derivation is not yet present in Scala 3 just yet (because I haven't figured out how to do it yet)
trait UdtValueDecoderMagnoliaDerivation extends ProductDerivation[CqlUdtValueDecoder]:
  final def join[T](ctx: CaseClass[Typeclass, T]): CqlUdtValueDecoder.Object[T] =
    (structure: UdtValue) =>
      ctx.construct { param =>
        val fieldName = CqlColumn.extractFieldName(param.annotations).getOrElse(param.label)
        val decoder   = param.typeclass
        decoder.decodeByFieldName(structure, fieldName)
      }

  inline def derive[A](using Mirror.Of[A]): CqlUdtValueDecoder.Object[A] =
    derived.asInstanceOf[CqlUdtValueDecoder.Object[A]]
