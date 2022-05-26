package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.UdtValueCursor

import scala.util.control.NonFatal

/**
 * A [[CqlUdtValueDecoder]] is a mechanism that provides a way to decode a
 * `UdtValue` into its component pieces (`A` being one of the components of the
 * `UdtValue`). This is really covariant in A but due to Magnolia we cannot mark
 * it as such as it interferes with automatic derivation.
 *
 * __Design note__: We can abstract over both CqlRowDecoder and
 * CqlUdtValueDecoder (lets call the abstraction: CqlStructureDecoder) because
 * UdtValue and Row share the same interface (GettableByName). If we do decide
 * to go down this path, we need to take special care in [[CqlPrimitiveDecoder]]
 * when it comes to UDTValues to ensure that we can materialize instances only
 * for `CqlStructureDecoder.WithDriver[A, UdtValue]` because we cannot have Rows
 * nested inside of Rows and cannot have that kind of implicit derivation play
 * out (it is valid to have nesting where Rows contain UdtValues and UdtValues
 * themselves contain UdtValues). We have to keep track of precise types (i.e.
 * UdtValue, Row) as using the interface GettableByName is not acceptable to the
 * Datastax driver. We currently take the approach of duplication to keep things
 * easier to read and understand for automatic derivation. Currently, RowDecoder
 * and UdtValueDecoder share many similarities however, nesting is not supported
 * in RowDecoder.
 */
trait CqlUdtValueDecoder[A] {
  def decodeByFieldName(structure: UdtValue, fieldName: String): A
  def decodeByIndex(structure: UdtValue, index: Int): A
}
object CqlUdtValueDecoder extends UdtValueDecoderMagnoliaDerivation {

  /**
   * A [[CqlUdtValueDecoder.Object]] is a mechanism that provides a way to
   * decode a `UdtValue` into a Scala type `A`. This is the public interface
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

    def either: CqlUdtValueDecoder.Object[Either[DecoderException, A]] =
      new CqlUdtValueDecoder.Object[Either[DecoderException, A]] {
        def decode(structure: UdtValue): Either[DecoderException, A] =
          try Right(self.decode(structure))
          catch {
            case NonFatal(decoderException: DecoderException) =>
              Left(decoderException)

            case NonFatal(cause) =>
              Left(
                DecoderException.StructureReadFailure(
                  message = s"Cannot decode UDT Value",
                  field = None,
                  structure = structure,
                  cause = cause
                )
              )
          }
      }

    def absolve[B](implicit ev: A <:< Either[DecoderException, B]): CqlUdtValueDecoder.Object[B] =
      new CqlUdtValueDecoder.Object[B] {
        def decode(structure: UdtValue): B =
          ev(self.decode(structure)) match {
            case Right(b)               => b
            case Left(decoderException) => throw decoderException
          }
      }

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

  def cursorEither[A](
    f: UdtValueCursor => Either[DecoderException, A]
  ): CqlUdtValueDecoder.Object[Either[DecoderException, A]] =
    new CqlUdtValueDecoder.Object[Either[DecoderException, A]] {
      override def decode(structure: UdtValue): Either[DecoderException, A] =
        f(UdtValueCursor(structure))
    }

  def cursor[A](f: UdtValueCursor => Either[DecoderException, A]): CqlUdtValueDecoder.Object[A] =
    cursorEither(f).absolve

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): CqlUdtValueDecoder[A] =
    new CqlUdtValueDecoder[A] {
      override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
        try CqlPrimitiveDecoder.decodePrimitiveByFieldName(structure, fieldName)
        catch {
          case NonFatal(decoderException: DecoderException) =>
            throw decoderException

          case NonFatal(cause) =>
            throw DecoderException.StructureReadFailure(
              message = s"Cannot decode field '$fieldName' in the UDT",
              field = Some(DecoderException.FieldType.Name(fieldName)),
              structure = structure,
              cause = cause
            )
        }

      override def decodeByIndex(structure: UdtValue, index: Int): A =
        try CqlPrimitiveDecoder.decodePrimitiveByIndex(structure, index)
        catch {
          case NonFatal(decoderException: DecoderException) =>
            throw decoderException

          case NonFatal(cause) =>
            throw DecoderException.StructureReadFailure(
              message = s"Cannot decode index $index in the UDT",
              field = Some(DecoderException.FieldType.Index(index)),
              structure = structure,
              cause = cause
            )
        }
    }

  implicit def tuple1UdtValueDecoder[A](implicit one: CqlPrimitiveDecoder[A]): CqlUdtValueDecoder.Object[Tuple1[A]] =
    new CqlUdtValueDecoder.Object[Tuple1[A]] {
      override def decode(udtValue: UdtValue): Tuple1[A] =
        Tuple1(CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one))
    }

  implicit def tuple2UdtValueDecoder[A, B](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B]
  ): CqlUdtValueDecoder.Object[(A, B)] =
    new CqlUdtValueDecoder.Object[(A, B)] {
      override def decode(udtValue: UdtValue): (A, B) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two)
        )
    }

  implicit def tuple3UdtValueDecoder[A, B, C](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C]
  ): CqlUdtValueDecoder.Object[(A, B, C)] =
    new CqlUdtValueDecoder.Object[(A, B, C)] {
      override def decode(udtValue: UdtValue): (A, B, C) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three)
        )
    }

  implicit def tuple4UdtValueDecoder[A, B, C, D](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D]
  ): CqlUdtValueDecoder.Object[(A, B, C, D)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D)] {
      override def decode(udtValue: UdtValue): (A, B, C, D) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four)
        )
    }

  implicit def tuple5UdtValueDecoder[A, B, C, D, E](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five)
        )
    }

  implicit def tuple6UdtValueDecoder[A, B, C, D, E, F](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six)
        )
    }

  implicit def tuple7UdtValueDecoder[A, B, C, D, E, F, G](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven)
        )
    }

  implicit def tuple8UdtValueDecoder[A, B, C, D, E, F, G, H](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight)
        )
    }

  implicit def tuple9UdtValueDecoder[A, B, C, D, E, F, G, H, I](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine)
        )
    }

  implicit def tuple10UdtValueDecoder[A, B, C, D, E, F, G, H, I, J](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten)
        )
    }

  implicit def tuple11UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven)
        )
    }

  implicit def tuple12UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve)
        )
    }

  implicit def tuple13UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen)
        )
    }

  implicit def tuple14UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen)
        )
    }

  implicit def tuple15UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen)
        )
    }

  implicit def tuple16UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen)
        )
    }

  implicit def tuple17UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen)
        )
    }

  implicit def tuple18UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q],
    eighteen: CqlPrimitiveDecoder[R]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 17)(eighteen)
        )
    }

  implicit def tuple19UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q],
    eighteen: CqlPrimitiveDecoder[R],
    nineteen: CqlPrimitiveDecoder[S]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 18)(nineteen)
        )
    }

  implicit def tuple20UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q],
    eighteen: CqlPrimitiveDecoder[R],
    nineteen: CqlPrimitiveDecoder[S],
    twenty: CqlPrimitiveDecoder[T]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 19)(twenty)
        )
    }

  implicit def tuple21UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q],
    eighteen: CqlPrimitiveDecoder[R],
    nineteen: CqlPrimitiveDecoder[S],
    twenty: CqlPrimitiveDecoder[T],
    twentyOne: CqlPrimitiveDecoder[U]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 19)(twenty),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 20)(twentyOne)
        )
    }

  implicit def tuple22UdtValueDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I],
    ten: CqlPrimitiveDecoder[J],
    eleven: CqlPrimitiveDecoder[K],
    twelve: CqlPrimitiveDecoder[L],
    thirteen: CqlPrimitiveDecoder[M],
    fourteen: CqlPrimitiveDecoder[N],
    fifteen: CqlPrimitiveDecoder[O],
    sixteen: CqlPrimitiveDecoder[P],
    seventeen: CqlPrimitiveDecoder[Q],
    eighteen: CqlPrimitiveDecoder[R],
    nineteen: CqlPrimitiveDecoder[S],
    twenty: CqlPrimitiveDecoder[T],
    twentyOne: CqlPrimitiveDecoder[U],
    twentyTwo: CqlPrimitiveDecoder[V]
  ): CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    new CqlUdtValueDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
      override def decode(udtValue: UdtValue): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 19)(twenty),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 20)(twentyOne),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = udtValue, index = 21)(twentyTwo)
        )
    }
}
