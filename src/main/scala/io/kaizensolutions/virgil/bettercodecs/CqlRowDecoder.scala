package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

import scala.util.control.NonFatal

/**
 * A [[CqlRowDecoder]] is an internal mechanism that provides a way to decode a
 * [[Row]] into its component pieces ([[A]] being one of the components of the
 * [[Row]]). This is really covariant in A but Magnolia will not automatically
 * derive if you mark it as such. The reason why its covariant is because if B
 * is a supertype of A, and you have a RowDecoder[A], A has more information
 * than B and B is really a subset of A, so you read out more information (A)
 * and discard information (since B contains less information than A)
 */
trait CqlRowDecoder[A] {
  private[virgil] def decodeByFieldName(row: Row, fieldName: String): A
  private[virgil] def decodeByIndex(row: Row, index: Int): A
}
object CqlRowDecoder extends RowDecoderMagnoliaDerivation {

  /**
   * A [[CqlRowDecoder.Object]] is a mechanism that provides a way to decode an
   * entire [[Row]] into some Scala type [[A]].
   *
   * NOTE: The automatic derivation mechanism and the custom method can produce
   * the following subtype. The automatic derivation mechanism uses
   * `fromCqlPrimitive` to create a [[CqlRowDecoder]] which knows how to extract
   * a component. We use Magnolia to build up Scala case classes from their
   * components
   *
   * @tparam A
   */
  trait Object[A] extends CqlRowDecoder[A] { self =>
    def decode(row: Row): A

    // You cannot have nested Rows within Rows
    // You can have nested UdtValues within Rows which is taken care of (using fromCqlPrimitive)
    // Magnolia dispatches to fromCqlPrimitive which generates instances and prevents recursion on this typeclass from taking place
    private[virgil] def decodeByFieldName(row: Row, fieldName: String): A = decode(row)
    private[virgil] def decodeByIndex(row: Row, index: Int): A            = decode(row)

    def map[B](f: A => B): CqlRowDecoder.Object[B] =
      new Object[B] {
        def decode(row: Row): B =
          f(self.decode(row))
      }

    def zipWith[B, C](other: CqlRowDecoder.Object[B])(f: (A, B) => C): CqlRowDecoder.Object[C] =
      new Object[C] {
        def decode(row: Row): C = {
          val a = self.decode(row)
          val b = other.decode(row)
          f(a, b)
        }
      }

    def zip[B](other: CqlRowDecoder.Object[B]): CqlRowDecoder.Object[(A, B)] =
      zipWith(other)((_, _))

    def either: CqlRowDecoder.Object[Either[DecoderException, A]] =
      new CqlRowDecoder.Object[Either[DecoderException, A]] {
        def decode(row: Row): Either[DecoderException, A] =
          try Right(self.decode(row))
          catch {
            case NonFatal(decoderException: DecoderException) =>
              Left(decoderException)

            case NonFatal(cause) =>
              Left(
                DecoderException.StructureReadFailure(
                  message = s"Cannot decode Row",
                  field = None,
                  structure = row,
                  cause = cause
                )
              )
          }
      }

    def absolve[B](implicit ev: A <:< Either[DecoderException, B]): CqlRowDecoder.Object[B] =
      new CqlRowDecoder.Object[B] {
        def decode(row: Row): B =
          ev(self.decode(row)) match {
            case Right(b)               => b
            case Left(decoderException) => throw decoderException
          }
      }

    def eitherWith[B, C](other: CqlRowDecoder.Object[B])(f: Either[A, B] => C): CqlRowDecoder.Object[C] =
      new Object[C] {
        def decode(row: Row): C = {
          val in =
            try { Left(self.decode(row)) }
            catch { case NonFatal(_) => Right(other.decode(row)) }
          f(in)
        }
      }

    def orElse(other: CqlRowDecoder.Object[A]): CqlRowDecoder.Object[A] =
      eitherWith(other)(_.merge)

    def orElseEither[B](other: CqlRowDecoder.Object[B]): CqlRowDecoder.Object[Either[A, B]] =
      eitherWith(other)(identity)

    def widen[B >: A]: CqlRowDecoder.Object[B] = self.map(identity)
  }

  // A user can only summon what is built by the automatic derivation mechanism
  def apply[A](implicit ev: CqlRowDecoder.Object[A]): CqlRowDecoder.Object[A] = ev

  def custom[A](f: Row => A): CqlRowDecoder.Object[A] = new CqlRowDecoder.Object[A] {
    override def decode(row: Row): A = f(row)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): CqlRowDecoder[A] = new CqlRowDecoder[A] {
    override def decodeByFieldName(row: Row, fieldName: String): A =
      try (CqlPrimitiveDecoder.decodePrimitiveByFieldName(row, fieldName))
      catch {
        case NonFatal(decoderException: DecoderException) =>
          throw decoderException

        case NonFatal(cause) =>
          throw DecoderException.StructureReadFailure(
            message = s"Cannot decode field '$fieldName' in the Row",
            field = Some(DecoderException.FieldType.Name(fieldName)),
            structure = row,
            cause = cause
          )
      }

    override def decodeByIndex(row: Row, index: Int): A =
      try (CqlPrimitiveDecoder.decodePrimitiveByIndex(row, index))
      catch {
        case NonFatal(decoderException: DecoderException) =>
          throw decoderException

        case NonFatal(cause) =>
          throw DecoderException.StructureReadFailure(
            message = s"Cannot decode index $index in the Row",
            field = Some(DecoderException.FieldType.Index(index)),
            structure = row,
            cause = cause
          )
      }
  }
}

trait RowDecoderMagnoliaDerivation {
  type Typeclass[T] = CqlRowDecoder[T]

  def join[T](ctx: CaseClass[CqlRowDecoder, T]): CqlRowDecoder.Object[T] =
    new CqlRowDecoder.Object[T] {
      override def decode(row: Row): T =
        ctx.construct { p =>
          val fieldName = CqlColumn.extractFieldName(p.annotations).getOrElse(p.label)
          p.typeclass.decodeByFieldName(row, fieldName)
        }
    }

  implicit def derive[T]: CqlRowDecoder.Object[T] = macro Magnolia.gen[T]
}
