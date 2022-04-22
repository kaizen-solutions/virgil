package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

trait RowDecoderMagnoliaDerivation {
  type Typeclass[T] = CqlRowDecoder[T]

  def join[T](ctx: CaseClass[CqlRowDecoder, T]): CqlRowDecoder.Object[T] =
    new CqlRowDecoder.Object[T] {
      override def decode(row: Row): T =
        ctx.construct { p =>
          val fieldName = CqlColumn.extractFieldName(p.annotations).getOrElse(p.label)
          p.typeclass.decodeByFieldName(row, fieldName)
        }
    }

  implicit def derive[T]: CqlRowDecoder.Object[T] = macro Magnolia.gen[T]
}
