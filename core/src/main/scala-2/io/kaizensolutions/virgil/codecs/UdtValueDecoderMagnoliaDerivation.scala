package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1.CaseClass
import magnolia1.Magnolia

trait UdtValueDecoderMagnoliaDerivation {
  type Typeclass[T] = CqlUdtValueDecoder[T]

  def join[T](ctx: CaseClass[CqlUdtValueDecoder, T]): CqlUdtValueDecoder.Object[T] =
    new CqlUdtValueDecoder.Object[T] {
      override def decode(structure: UdtValue): T =
        ctx.construct { param =>
          val fieldName = CqlColumn.extractFieldName(param.annotations).getOrElse(param.label)
          val decoder   = param.typeclass
          decoder.decodeByFieldName(structure, fieldName)
        }
    }

  implicit def derive[T]: CqlUdtValueDecoder.Object[T] = macro Magnolia.gen[T]
}
