package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.annotations.{CqlColumn, CqlDiscriminator, CqlSubtype}
import zio.Chunk
import zio.schema.Schema

import scala.annotation.implicitNotFound

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
@implicitNotFound(
  "No CqlDecoder found for type ${A}, if you are using java.time types or java.nio.ByteBuffer or CqlDuration, import codecs._ to get instances and ensure a zio.schema.Schema[${A}] can be found"
)
trait CqlDecoder[A] { self =>
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

  def orElse(that: CqlDecoder[A]): CqlDecoder[A] = new CqlDecoder[A] {
    override def decode(row: Row): Either[String, A] =
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
object CqlDecoder {

  /**
   * CREATE TYPE IF NOT EXISTS address ( street TEXT, city TEXT, points
   * frozen<map<TEXT, set<set<point>>>> );
   */
  // final case class Person(address: Address)
  // final case class Address(street: String, city2: String, randomField: List[Int]) ->

  implicit val rowCqlDecoder: CqlDecoder[Row] =
    new CqlDecoder[Row] {
      override def decode(row: Row): Either[String, Row] = Right(row)
    }

  implicit def derive[A](implicit schema: Schema[A]): CqlDecoder[A] = schema match {
    case prim: Schema.Primitive[a] =>
      val columnDecoder = CqlColumnDecoder.fromSchema(prim)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          eitherConvert(columnDecoder.decodeFieldByIndex(row, 0))
      }

    case `byteBufferSchema` =>
      val columnDecoder = CqlColumnDecoder.byteBufferColumnDecoder
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          eitherConvert(columnDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A])
      }

    case `cqlDurationSchema` =>
      val columnDecoder = CqlColumnDecoder.cqlDurationColumnDecoder
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          eitherConvert(columnDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A])
      }

    case record: Schema.Record[a] =>
      new CqlDecoder[A] {
        private val fieldDecoders =
          record.structure.map { field =>
            val fieldName     = CqlColumn.extractFieldName(field.annotations).getOrElse(field.label)
            val columnDecoder = CqlColumnDecoder.fromSchema(field.schema)
            (fieldName, columnDecoder)
          }

        override def decode(row: Row): Either[String, A] = {
          val lowLevelDecoderResult: Either[String, Either[String, A]] =
            eitherConvert {
              record.rawConstruct {
                // this can throw but I don't want to wrap each of these in eitherConvert for performance
                fieldDecoders.map { case (fieldName, decoder) =>
                  decoder.decodeFieldByName(row, fieldName)
                }
              }.map(_.asInstanceOf[A])
            }

          lowLevelDecoderResult.flatMap(identity)
        }
      }

    case Schema.Lazy(s) =>
      derive(s())

    case e: Schema.EitherSchema[l, r] =>
      val ld = CqlDecoder.derive(e.left)
      val rd = CqlDecoder.derive(e.right)
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
          eitherConvert(listDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A])
      }

    case Schema.SetSchema(elementSchema, _) =>
      val elementDecoder = CqlColumnDecoder.fromSchema(elementSchema)
      val setDecoder     = CqlColumnDecoder.setColumnDecoder(elementDecoder)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          eitherConvert(setDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A])
      }

    case Schema.MapSchema(keySchema, valueSchema, _) =>
      val keyDecoder   = CqlColumnDecoder.fromSchema(keySchema)
      val valueDecoder = CqlColumnDecoder.fromSchema(valueSchema)
      val mapDecoder   = CqlColumnDecoder.mapColumnDecoder(keyDecoder, valueDecoder)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          eitherConvert(mapDecoder.decodeFieldByIndex(row, 0).asInstanceOf[A])
      }

    case Schema.Optional(schema, _) =>
      val decoder = CqlDecoder.derive(schema)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          decoder.decode(row).map(Option(_))
      }

    case Schema.Transform(schema, to, _, _) =>
      val decoder = CqlDecoder.derive(schema)
      new CqlDecoder[A] {
        override def decode(row: Row): Either[String, A] =
          decoder.decode(row).flatMap(to(_))
      }

    case enum: Schema.Enum[a] =>
      val discriminator = CqlDiscriminator.extract(enum.annotations)
      discriminator match {
        case Some(disc) =>
          // Cache the derivation of each enum case
          val enumDecoderByKeyType: Map[String, CqlDecoder[_]] =
            enum.structure.toList.map { case (defaultTypeName, schema) =>
              val typeName =
                CqlSubtype
                  .extract(schema.annotations)
                  .getOrElse(defaultTypeName)

              typeName -> CqlDecoder.derive(schema)
            }.toMap

          discriminatorBasedEnumDecoder(disc, enumDecoderByKeyType)

        case None =>
          val decoders = enum.structure.map { case (_, schema) => CqlDecoder.derive(schema) }
          new CqlDecoder[A] {
            override def decode(row: Row): Either[String, A] =
              decodeFirstMatching(
                row = row,
                in = decoders,
                errors = Chunk.empty
              ).map(_.asInstanceOf[A])
          }
      }

    case Schema.Tuple(_, _, _) =>
      throw new RuntimeException(
        "CqlDecoder.derive does not support Tuples, please use this only for case classes or other primitives and collections"
      )

    case Schema.Fail(message, _) =>
      throw new RuntimeException(s"CqlDecoder encountered a Schema.Fail: $message")

    case m @ Schema.Meta(_, _) =>
      throw new RuntimeException(s"CqlDecoder encountered a Schema.Meta with is not supported $m")

    case other => throw new RuntimeException(s"Cannot derive CqlDecoder for $other")
  }

  private def discriminatorBasedEnumDecoder[A](
    discriminatorField: String,
    decoderLookup: Map[String, CqlDecoder[_]]
  ): CqlDecoder[A] = {

    def errorDuringLookup(in: String) =
      s"Unknown enum type: $in, possible values (${decoderLookup.keys.mkString(", ")})"

    new CqlDecoder[A] {
      override def decode(row: Row): Either[String, A] = for {
        discriminator <- eitherConvert(row.getString(discriminatorField))
        a <- decoderLookup
               .get(discriminator)
               .toRight(errorDuringLookup(discriminator))
               .flatMap(_.decode(row))
        result = a.asInstanceOf[A]
      } yield result
    }
  }

  @scala.annotation.tailrec
  private def decodeFirstMatching(row: Row, in: Iterable[CqlDecoder[_]], errors: Chunk[String]): Either[String, _] =
    if (in.isEmpty) Left(errors.mkString("Tried the following decoders: ", " and then tried ", ""))
    else {
      val head = in.head
      head.decode(row) match {
        case Left(value)  => decodeFirstMatching(row, in.tail, errors :+ value)
        case Right(value) => Right(value)
      }
    }

}
