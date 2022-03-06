package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

import scala.util.control.NonFatal

/**
 * A [[CqlUdtValueDecoder]] is a mechanism that provides a way to decode a
 * [[UdtValue]] into its component pieces ([[A]] being one of the components of
 * the [[UdtValue]]). This is really covariant in A but due to Magnolia we
 * cannot mark it as such as it interferes with automatic derivation
 */
trait CqlUdtValueDecoder[A] {
  def decodeByFieldName(structure: UdtValue, fieldName: String): A
  def decodeByIndex(structure: UdtValue, index: Int): A
}
object CqlUdtValueDecoder extends UdtValueDecoderMagnoliaDerivation {

  /**
   * A [[CqlUdtValueDecoder.Object]] is a mechanism that provides a way to
   * decode a [[UdtValue]] into a Scala type [[A]]. This is the public interface
   * exposed to the user
   */
  trait Object[A] extends CqlUdtValueDecoder[A] { self =>
    def decode(structure: UdtValue): A

    override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
      decode(structure.getUdtValue(fieldName))

    override def decodeByIndex(structure: UdtValue, index: Int): A =
      decode(structure.getUdtValue(index))

    def map[B](f: A => B): CqlUdtValueDecoder.Object[B] =
      new Object[B] {
        def decode(structure: UdtValue): B =
          f(self.decode(structure))
      }

    def zipWith[B, C](other: CqlUdtValueDecoder.Object[B])(f: (A, B) => C): CqlUdtValueDecoder.Object[C] =
      new Object[C] {
        def decode(structure: UdtValue): C = {
          val a = self.decode(structure)
          val b = other.decode(structure)
          f(a, b)
        }
      }

    def zip[B](other: CqlUdtValueDecoder.Object[B]): CqlUdtValueDecoder.Object[(A, B)] =
      zipWith(other)((_, _))

    def eitherWith[B, C](other: CqlUdtValueDecoder.Object[B])(f: Either[A, B] => C): CqlUdtValueDecoder.Object[C] =
      new Object[C] {
        def decode(structure: UdtValue): C = {
          val in =
            try { Left(self.decode(structure)) }
            catch { case NonFatal(_) => Right(other.decode(structure)) }
          f(in)
        }
      }

    def orElse(other: CqlUdtValueDecoder.Object[A]): CqlUdtValueDecoder.Object[A] =
      eitherWith(other)(_.merge)

    def orElseEither[B](other: CqlUdtValueDecoder.Object[B]): CqlUdtValueDecoder.Object[Either[A, B]] =
      eitherWith(other)(identity)

    def widen[B >: A]: CqlUdtValueDecoder.Object[B] = self.map(identity)
  }

  def apply[A](implicit ev: CqlUdtValueDecoder.Object[A]): CqlUdtValueDecoder.Object[A] = ev

  def custom[A](f: UdtValue => A): CqlUdtValueDecoder.Object[A] = new CqlUdtValueDecoder.Object[A] {
    override def decode(structure: UdtValue): A = f(structure)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): CqlUdtValueDecoder[A] =
    new CqlUdtValueDecoder[A] {
      override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
        try (CqlPrimitiveDecoder.decodePrimitiveByFieldName(structure, fieldName))
        catch {
          case NonFatal(cause) =>
            throw DecoderException(
              message = s"Cannot decode field '$fieldName' in the UDT",
              field = FieldType.Name(fieldName),
              structure = structure,
              cause = cause
            )
        }

      override def decodeByIndex(structure: UdtValue, index: Int): A =
        try (CqlPrimitiveDecoder.decodePrimitiveByIndex(structure, index))
        catch {
          case NonFatal(cause) =>
            throw DecoderException(
              message = s"Cannot decode index $index in the UDT",
              field = FieldType.Index(index),
              structure = structure,
              cause = cause
            )
        }
    }
}
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
