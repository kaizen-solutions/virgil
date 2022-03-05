package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.cql.Row
import magnolia1._

import scala.util.control.NonFatal

/**
 * A [[RowDecoder]] is an internal mechanism that provides a way to decode a
 * [[Row]] into its component pieces ([[A]] being one of the components of the
 * [[Row]]).
 */
trait RowDecoder[A] {
  private[virgil] def decodeByFieldName(row: Row, fieldName: String): A
  private[virgil] def decodeByIndex(row: Row, index: Int): A
}
object RowDecoder extends RowDecoderMagnoliaDerivation {

  /**
   * A [[RowDecoder.Object]] is a mechanism that provides a way to decode an
   * entire [[Row]] into some Scala type [[A]].
   *
   * NOTE: The automatic derivation mechanism and the custom method can produce
   * the following subtype. The automatic derivation mechanism uses
   * `fromCqlPrimitive` to create a [[RowDecoder]] which knows how to extract a
   * component. We use Magnolia to build up Scala case classes from their
   * components
   *
   * @tparam A
   */
  trait Object[A] extends RowDecoder[A] { self =>
    def decode(row: Row): A

    // You cannot have nested Rows within Rows
    // You can have nested UdtValues within Rows which is taken care of (using fromCqlPrimitive)
    // Magnolia dispatches to fromCqlPrimitive which generates instances and prevents recursion on this typeclass from taking place
    private[virgil] def decodeByFieldName(row: Row, fieldName: String): A = decode(row)
    private[virgil] def decodeByIndex(row: Row, index: Int): A            = decode(row)

    def map[B](f: A => B): RowDecoder.Object[B] =
      new Object[B] {
        def decode(row: Row): B =
          f(self.decode(row))
      }

    def zipWith[B, C](other: RowDecoder.Object[B])(f: (A, B) => C): RowDecoder.Object[C] =
      new Object[C] {
        def decode(row: Row): C = {
          val a = self.decode(row)
          val b = other.decode(row)
          f(a, b)
        }
      }

    def zip[B](other: RowDecoder.Object[B]): RowDecoder.Object[(A, B)] =
      zipWith(other)((_, _))

    def eitherWith[B, C](other: RowDecoder.Object[B])(f: Either[A, B] => C): RowDecoder.Object[C] =
      new Object[C] {
        def decode(row: Row): C = {
          val in =
            try { Left(self.decode(row)) }
            catch { case NonFatal(_) => Right(other.decode(row)) }
          f(in)
        }
      }

    def orElse(other: RowDecoder.Object[A]): RowDecoder.Object[A] =
      eitherWith(other)(_.merge)

    def orElseEither[B](other: RowDecoder.Object[B]): RowDecoder.Object[Either[A, B]] =
      eitherWith(other)(identity)
  }

  // A user can only summon what is built by the automatic derivation mechanism
  def apply[A](implicit ev: RowDecoder.Object[A]): RowDecoder.Object[A] = ev

  def custom[A](f: Row => A): RowDecoder.Object[A] = new RowDecoder.Object[A] {
    override def decode(row: Row): A = f(row)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): RowDecoder[A] = new RowDecoder[A] {
    override def decodeByFieldName(row: Row, fieldName: String): A =
      CqlPrimitiveDecoder.decodePrimitiveByFieldName(row, fieldName)

    override def decodeByIndex(row: Row, index: Int): A =
      CqlPrimitiveDecoder.decodePrimitiveByIndex(row, index)
  }
}
trait RowDecoderMagnoliaDerivation {
  type Typeclass[T] = RowDecoder[T]

  def join[T](ctx: CaseClass[RowDecoder, T]): RowDecoder.Object[T] =
    new RowDecoder.Object[T] {
      override def decode(row: Row): T =
        ctx.construct { p =>
          val fieldName = p.label
          p.typeclass.decodeByFieldName(row, fieldName)
        }
    }

  implicit def derive[T]: RowDecoder.Object[T] = macro Magnolia.gen[T]
}
