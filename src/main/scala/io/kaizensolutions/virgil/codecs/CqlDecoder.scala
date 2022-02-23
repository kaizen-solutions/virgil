package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.annotations
import magnolia1._

/**
 * CQL Decoder gives us the ability to read from a Cassandra row and convert it
 * to a case class.
 *
 * Usage:
 * {{{
 *    final case class Person(id: Int, name: String, age: Int)
 *    object Person {
 *      implicit val decoder: Decoder[Person] = Decoder.derive[Person]
 *    }
 * }}}
 */
object CqlDecoder {
  type Typeclass[T] = CqlColumnDecoder[T]

  def join[T](ctx: CaseClass[CqlColumnDecoder, T]): CqlColumnDecoder.WithDriver[T, Row] =
    CqlColumnDecoder.fromRow { row =>
      ctx.construct { param =>
        val fieldName = {
          val default = param.label
          annotations.CqlColumn.extractFieldName(param.annotations).getOrElse(default)
        }
        val reader = param.typeclass
        reader.decodeFieldByName(row, fieldName)
      }
    }

  // We cannot make this implicit as it would produce incorrect nested Readers choosing Row instead of UdtValue as the DriverType
  // causing the driver to break as it depends heavily on class types
  def derive[T]: CqlColumnDecoder.WithDriver[T, Row] = macro Magnolia.gen[T]
}
