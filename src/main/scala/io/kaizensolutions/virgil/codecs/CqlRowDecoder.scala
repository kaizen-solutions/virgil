package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.RowCursor
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

import scala.util.control.NonFatal

/**
 * A [[CqlRowDecoder]] is an internal mechanism that provides a way to decode a
 * `Row` into its component pieces (`A` being one of the components of the
 * `Row`). This is really covariant in A but Magnolia will not automatically
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
   * entire `Row` into some Scala type `A`.
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

  def cursorEither[A](f: RowCursor => Either[DecoderException, A]): CqlRowDecoder.Object[Either[DecoderException, A]] =
    new CqlRowDecoder.Object[Either[DecoderException, A]] {
      override def decode(row: Row): Either[DecoderException, A] =
        f(RowCursor(row))
    }

  def cursor[A](f: RowCursor => Either[DecoderException, A]): CqlRowDecoder.Object[A] =
    cursorEither(f).absolve

  implicit val cqlRowDecoderForUnderlyingRow: CqlRowDecoder.Object[Row] =
    new CqlRowDecoder.Object[Row] {
      override def decode(row: Row): Row = row
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

  implicit def tuple1RowDecoder[A](implicit one: CqlPrimitiveDecoder[A]): CqlRowDecoder.Object[Tuple1[A]] =
    new CqlRowDecoder.Object[Tuple1[A]] {
      override def decode(row: Row): Tuple1[A] =
        Tuple1(CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one))
    }

  implicit def tuple2RowDecoder[A, B](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B]
  ): CqlRowDecoder.Object[(A, B)] =
    new CqlRowDecoder.Object[(A, B)] {
      override def decode(row: Row): (A, B) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two)
        )
    }

  implicit def tuple3RowDecoder[A, B, C](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C]
  ): CqlRowDecoder.Object[(A, B, C)] =
    new CqlRowDecoder.Object[(A, B, C)] {
      override def decode(row: Row): (A, B, C) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three)
        )
    }

  implicit def tuple4RowDecoder[A, B, C, D](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D]
  ): CqlRowDecoder.Object[(A, B, C, D)] =
    new CqlRowDecoder.Object[(A, B, C, D)] {
      override def decode(row: Row): (A, B, C, D) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four)
        )
    }

  implicit def tuple5RowDecoder[A, B, C, D, E](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E]
  ): CqlRowDecoder.Object[(A, B, C, D, E)] =
    new CqlRowDecoder.Object[(A, B, C, D, E)] {
      override def decode(row: Row): (A, B, C, D, E) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five)
        )
    }

  implicit def tuple6RowDecoder[A, B, C, D, E, F](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F]
  ): CqlRowDecoder.Object[(A, B, C, D, E, F)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F)] {
      override def decode(row: Row): (A, B, C, D, E, F) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six)
        )
    }

  implicit def tuple7RowDecoder[A, B, C, D, E, F, G](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G]
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G)] {
      override def decode(row: Row): (A, B, C, D, E, F, G) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven)
        )
    }

  implicit def tuple8RowDecoder[A, B, C, D, E, F, G, H](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H]
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight)
        )
    }

  implicit def tuple9RowDecoder[A, B, C, D, E, F, G, H, I](implicit
    one: CqlPrimitiveDecoder[A],
    two: CqlPrimitiveDecoder[B],
    three: CqlPrimitiveDecoder[C],
    four: CqlPrimitiveDecoder[D],
    five: CqlPrimitiveDecoder[E],
    six: CqlPrimitiveDecoder[F],
    seven: CqlPrimitiveDecoder[G],
    eight: CqlPrimitiveDecoder[H],
    nine: CqlPrimitiveDecoder[I]
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine)
        )
    }

  implicit def tuple10RowDecoder[A, B, C, D, E, F, G, H, I, J](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten)
        )
    }

  implicit def tuple11RowDecoder[A, B, C, D, E, F, G, H, I, J, K](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven)
        )
    }

  implicit def tuple12RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve)
        )
    }

  implicit def tuple13RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen)
        )
    }

  implicit def tuple14RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen)
        )
    }

  implicit def tuple15RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen)
        )
    }

  implicit def tuple16RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen)
        )
    }

  implicit def tuple17RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen)
        )
    }

  implicit def tuple18RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 17)(eighteen)
        )
    }

  implicit def tuple19RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 18)(nineteen)
        )
    }

  implicit def tuple20RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 19)(twenty)
        )
    }

  implicit def tuple21RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 19)(twenty),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 20)(twentyOne)
        )
    }

  implicit def tuple22RowDecoder[A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit
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
  ): CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] =
    new CqlRowDecoder.Object[(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] {
      override def decode(row: Row): (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V) =
        (
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 0)(one),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 1)(two),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 2)(three),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 3)(four),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 4)(five),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 5)(six),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 6)(seven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 7)(eight),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 8)(nine),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 9)(ten),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 10)(eleven),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 11)(twelve),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 12)(thirteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 13)(fourteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 14)(fifteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 15)(sixteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 16)(seventeen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 17)(eighteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 18)(nineteen),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 19)(twenty),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 20)(twentyOne),
          CqlPrimitiveDecoder.decodePrimitiveByIndex(structure = row, index = 21)(twentyTwo)
        )
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
