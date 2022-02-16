package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import magnolia1._

object RowReader {
  type Typeclass[T] = Reader[T]

  def join[T](ctx: CaseClass[Reader, T]): Reader.WithDriver[T, Row] =
    Reader.fromRow { row =>
      ctx.construct { param =>
        val fieldName = param.label
        val reader    = param.typeclass
        reader.read(row, Option(fieldName))
      }
    }

  def derive[T]: Reader.WithDriver[T, Row] = macro Magnolia.gen[T]
}
