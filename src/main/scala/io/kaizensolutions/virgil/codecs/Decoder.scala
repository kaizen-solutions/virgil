package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import magnolia1._

/**
 * Decoder gives us the ability to read from a Cassandra row and convert it to a
 * case class.
 *
 * Usage:
 * {{{
 *    final case class Person(id: Int, name: String, age: Int)
 *    object Person {
 *      implicit val decoder: Decoder[Person] = Decoder.derive[Person]
 *    }
 * }}}
 */
object Decoder {
  type Typeclass[T] = ColumnDecoder[T]

  def join[T](ctx: CaseClass[ColumnDecoder, T]): ColumnDecoder.WithDriver[T, Row] =
    ColumnDecoder.fromRow { row =>
      ctx.construct { param =>
        val fieldName = param.label
        val reader    = param.typeclass
        reader.decodeFieldByName(row, fieldName)
      }
    }

  // We cannot make this implicit as it would produce incorrect nested Readers choosing Row instead of UdtValue as the DriverType
  // causing the driver to break as it depends heavily on class types
  def derive[T]: ColumnDecoder.WithDriver[T, Row] = macro Magnolia.gen[T]
}
