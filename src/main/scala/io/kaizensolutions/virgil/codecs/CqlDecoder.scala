package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import zio.schema.Schema

import scala.annotation.tailrec

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
trait CqlDecoder[+A] {
  def decode(row: Row): Either[String, A]
}
object CqlDecoder {
  implicit val rowCqlDecoder: CqlDecoder[Row] = (row: Row) => Right(row)

  @tailrec
  def derive[A](implicit schema: Schema[A]): CqlDecoder[A] = schema match {
    case record: Schema.Record[a] =>
      new CqlDecoder[A] {
        // cached
        private val fieldDecoders =
          record.structure.map { field =>
            val columnDecoder = CqlColumnDecoder.fromSchema(field.schema)
            (field.label, columnDecoder)
          }

        override def decode(row: Row): Either[String, A] =
          record.rawConstruct {
            fieldDecoders.map { case (fieldName, decoder) =>
              decoder.decodeFieldByName(row, fieldName)
            }
          }.map(_.asInstanceOf[A])
      }

    case Schema.Lazy(s) => derive(s())
    case other          => throw new RuntimeException(s"Cannot derive CqlDecoder for $other")
//    case enum: Schema.Enum[_] => ???
//    case collection: Schema.Collection[_, _] => ???
//    case Schema.Transform(codec, f, g, annotations) => ???
//    case Schema.Primitive(standardType, annotations) => ???
//    case Schema.Optional(codec, annotations) => ???
//    case Schema.Fail(message, annotations) => ???
//    case Schema.Tuple(left, right, annotations) => ???
//    case Schema.EitherSchema(left, right, annotations) => ???
//    case Schema.Meta(ast, annotations) => ???
  }
}
