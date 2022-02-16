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
        reader.read(row, fieldName)
      }
    }

  // We cannot make this implicit as it would produce incorrect nested Readers choosing Row instead of UdtValue as the DriverType
  // causing the driver to break as it depends heavily on class types
  def derive[T]: Reader.WithDriver[T, Row] = macro Magnolia.gen[T]
}
