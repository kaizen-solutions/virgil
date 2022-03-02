package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import zio.schema.Schema

import scala.util.control.NonFatal

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
trait CqlDecoder[+A] { self =>
  def decode(row: Row): Either[String, A]

  def map[B](f: A => B): CqlDecoder[B] = new CqlDecoder[B] {
    override def decode(row: Row): Either[String, B] =
      self.decode(row).map(f)
  }

  def zipWith[B, C](that: CqlDecoder[B])(f: (A, B) => C): CqlDecoder[C] =
    new CqlDecoder[C] {
      override def decode(row: Row): Either[String, C] =
        for {
          a <- self.decode(row)
          b <- that.decode(row)
        } yield f(a, b)
    }

  def zip[B](that: CqlDecoder[B]): CqlDecoder[(A, B)] = zipWith(that)((_, _))

  def orElse[A1 >: A](that: CqlDecoder[A1]): CqlDecoder[A1] = new CqlDecoder[A1] {
    override def decode(row: Row): Either[String, A1] =
      self.decode(row) match {
        case Left(fstError) =>
          that
            .decode(row)
            .left
            .map(sndError => s"orElse: $fstError then $sndError")

        case Right(ok) =>
          Right(ok)
      }
  }

  def orElseEither[B](that: CqlDecoder[B]): CqlDecoder[Either[A, B]] = new CqlDecoder[Either[A, B]] {
    override def decode(row: Row): Either[String, Either[A, B]] =
      self.decode(row) match {
        case Left(fstError) =>
          that
            .decode(row)
            .left
            .map(sndError => s"orElseEither: $fstError then $sndError")
            .map(Right(_))

        case Right(ok) =>
          Right(Left(ok))
      }
  }
}
object CqlDecoder extends CqlDecoderDerivation {
  implicit val rowCqlDecoder: CqlDecoder[Row] = (row: Row) => Right(row)
}

trait CqlDecoderDerivation {
  implicit def derive[A](implicit schema: Schema[A]): CqlDecoder[A] =
    fromSchema()(schema)

  def fromSchema[A](index: Int = 0)(implicit schema: Schema[A]): CqlDecoder[A] = schema match {
    case prim: Schema.Primitive[a] =>
      val columnDecoder = CqlColumnDecoder.fromSchema(prim)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          try { Right(columnDecoder.decodeFieldByIndex(row, index).asInstanceOf[A]) }
          catch {
            case NonFatal(e) => Left(e.getMessage)
          }
      }

    case `cqlDurationSchema` =>
      val columnDecoder = CqlColumnDecoder.cqlDurationColumnDecoder
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          try { Right(columnDecoder.decodeFieldByIndex(row, index).asInstanceOf[A]) }
          catch { case NonFatal(e) => Left(e.getMessage) }
      }

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

    case Schema.Lazy(s) =>
      fromSchema(index)(s())

    case e: Schema.EitherSchema[l, r] =>
      val ld = CqlDecoder.fromSchema(index)(e.left)
      val rd = CqlDecoder.fromSchema(index)(e.right)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] = {
          val l = ld.decode(row)
          l match {
            case Left(_)   => rd.decode(row).map(Right(_))
            case Right(ok) => Right(Left(ok))
          }
        }.map(_.asInstanceOf[A])
      }

    case Schema.Sequence(elementSchema, _, _, _) =>
      val elementDecoder = CqlColumnDecoder.fromSchema(elementSchema)
      val listDecoder    = CqlColumnDecoder.listColumnDecoder(elementDecoder)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          try { Right(listDecoder.decodeFieldByIndex(row, index).asInstanceOf[A]) }
          catch { case NonFatal(e) => Left(e.getMessage) }
      }

    case Schema.SetSchema(elementSchema, _) =>
      val elementDecoder = CqlColumnDecoder.fromSchema(elementSchema)
      val setDecoder     = CqlColumnDecoder.setColumnDecoder(elementDecoder)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          try { Right(setDecoder.decodeFieldByIndex(row, index).asInstanceOf[A]) }
          catch { case NonFatal(e) => Left(e.getMessage) }
      }

    case Schema.MapSchema(keySchema, valueSchema, _) =>
      val keyDecoder   = CqlColumnDecoder.fromSchema(keySchema)
      val valueDecoder = CqlColumnDecoder.fromSchema(valueSchema)
      val mapDecoder   = CqlColumnDecoder.mapColumnDecoder(keyDecoder, valueDecoder)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          try { Right(mapDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A]) }
          catch { case NonFatal(e) => Left(e.getMessage) }
      }

    case Schema.Optional(schema, _) =>
      val decoder = CqlDecoder.fromSchema(index)(schema)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          decoder.decode(row).map(Option(_))
      }

    case Schema.Transform(schema, to, _, _) =>
      val decoder = CqlDecoder.fromSchema(index)(schema)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          decoder.decode(row).flatMap(to(_))
      }

    case enum: Schema.Enum[a] =>
      val enumDecoder =
        enum.structure.map { case (_, schema) => CqlDecoder.fromSchema(index)(schema) }
          .reduce(_ orElse _)

      enumDecoder.map(_.asInstanceOf[A])

    case Schema.Tuple(_, _, _) =>
      throw new RuntimeException("CqlDecoder does not support Tuples, please use case classes instead")

    case Schema.Fail(message, _) =>
      throw new RuntimeException(s"CqlDecoder encountered a Schema.Fail: $message")

    case m @ Schema.Meta(_, _) =>
      throw new RuntimeException(s"CqlDecoder encountered a Schema.Meta with is not supported $m")

    case other => throw new RuntimeException(s"Cannot derive CqlDecoder for $other")
  }
}
