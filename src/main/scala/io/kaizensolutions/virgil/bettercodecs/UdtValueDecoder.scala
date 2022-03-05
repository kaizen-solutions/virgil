package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

import scala.util.control.NonFatal

/**
 * A [[UdtValueDecoder]] is a mechanism that provides a way to decode a
 * [[UdtValue]] into its component pieces ([[A]] being one of the components of
 * the [[UdtValue]]).
 */
trait UdtValueDecoder[A] {
  def decodeByFieldName(structure: UdtValue, fieldName: String): A
  def decodeByIndex(structure: UdtValue, index: Int): A
}
object UdtValueDecoder extends UdtValueDecoderMagnoliaDerivation {

  /**
   * A [[UdtValueDecoder.Object]] is a mechanism that provides a way to decode a
   * [[UdtValue]] into a Scala type [[A]]. This is the public interface exposed
   * to the user
   */
  trait Object[A] extends UdtValueDecoder[A] { self =>
    def decode(structure: UdtValue): A

    override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
      decode(structure.getUdtValue(fieldName))

    override def decodeByIndex(structure: UdtValue, index: Int): A =
      decode(structure.getUdtValue(index))

    def map[B](f: A => B): UdtValueDecoder.Object[B] =
      new Object[B] {
        def decode(structure: UdtValue): B =
          f(self.decode(structure))
      }

    def zipWith[B, C](other: UdtValueDecoder.Object[B])(f: (A, B) => C): UdtValueDecoder.Object[C] =
      new Object[C] {
        def decode(structure: UdtValue): C = {
          val a = self.decode(structure)
          val b = other.decode(structure)
          f(a, b)
        }
      }

    def zip[B](other: UdtValueDecoder.Object[B]): UdtValueDecoder.Object[(A, B)] =
      zipWith(other)((_, _))

    def eitherWith[B, C](other: UdtValueDecoder.Object[B])(f: Either[A, B] => C): UdtValueDecoder.Object[C] =
      new Object[C] {
        def decode(structure: UdtValue): C = {
          val in =
            try { Left(self.decode(structure)) }
            catch { case NonFatal(_) => Right(other.decode(structure)) }
          f(in)
        }
      }

    def orElse(other: UdtValueDecoder.Object[A]): UdtValueDecoder.Object[A] =
      eitherWith(other)(_.merge)

    def orElseEither[B](other: UdtValueDecoder.Object[B]): UdtValueDecoder.Object[Either[A, B]] =
      eitherWith(other)(identity)
  }

  def apply[A](implicit ev: UdtValueDecoder.Object[A]): UdtValueDecoder.Object[A] = ev

  def custom[A](f: UdtValue => A): UdtValueDecoder.Object[A] = new UdtValueDecoder.Object[A] {
    override def decode(structure: UdtValue): A = f(structure)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): UdtValueDecoder[A] =
    new UdtValueDecoder[A] {
      override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
        CqlPrimitiveDecoder.decodePrimitiveByFieldName(structure, fieldName)

      override def decodeByIndex(structure: UdtValue, index: Int): A =
        CqlPrimitiveDecoder.decodePrimitiveByIndex(structure, index)
    }
}
trait UdtValueDecoderMagnoliaDerivation {
  type Typeclass[T] = UdtValueDecoder[T]

  def join[T](ctx: CaseClass[UdtValueDecoder, T]): UdtValueDecoder.Object[T] =
    new UdtValueDecoder.Object[T] {
      override def decode(structure: UdtValue): T =
        ctx.construct { param =>
          val fieldName = param.label
          val decoder   = param.typeclass
          decoder.decodeByFieldName(structure, fieldName)
        }
    }

  implicit def derive[T]: UdtValueDecoder.Object[T] = macro Magnolia.gen[T]
}
