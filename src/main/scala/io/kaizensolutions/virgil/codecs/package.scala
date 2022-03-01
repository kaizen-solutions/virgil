package io.kaizensolutions.virgil

package object codecs {
  def combine[A, B](a: Either[List[String], A], b: Either[List[String], B]): Either[List[String], (A, B)] =
    (a, b) match {
      case (Right(a), Right(b)) => Right((a, b))
      case (Left(a), Left(b))   => Left(a ++ b)
      case (Left(a), _)         => Left(a)
      case (_, Left(b))         => Left(b)
    }
}
