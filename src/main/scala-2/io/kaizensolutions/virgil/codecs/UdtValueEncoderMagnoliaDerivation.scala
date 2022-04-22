package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

trait UdtValueEncoderMagnoliaDerivation {
  type Typeclass[T] = CqlUdtValueEncoder[T]

  def join[T](ctx: CaseClass[CqlUdtValueEncoder, T]): CqlUdtValueEncoder.Object[T] =
    new CqlUdtValueEncoder.Object[T] {
      override def encode(structure: UdtValue, value: T): UdtValue =
        ctx.parameters.foldLeft(structure) { case (acc, param) =>
          val fieldName  = CqlColumn.extractFieldName(param.annotations).getOrElse(param.label)
          val fieldValue = param.dereference(value)
          val encoder    = param.typeclass
          encoder.encodeByFieldName(acc, fieldName, fieldValue)
        }
    }

  implicit def derive[T]: CqlUdtValueEncoder.Object[T] = macro Magnolia.gen[T]
}
