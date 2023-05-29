package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlPrimitiveDecoder
import io.kaizensolutions.virgil.codecs.CqlPrimitiveEncoder

final case class MutationResult private (result: Boolean) extends AnyVal
object MutationResult {
  def make(result: Boolean): MutationResult = MutationResult(result)

  // Make it such that you cannot accidentally create a Query of a MutationResult because this is an invalid state
  implicit val cqlCqlPrimitiveEncoderAmbiguous1: CqlPrimitiveEncoder[MutationResult] =
    CqlPrimitiveEncoder[Boolean].contramap(_.result)

  implicit val cqlColumnEncoderForMutationResultAmbiguous2: CqlPrimitiveEncoder[MutationResult] =
    CqlPrimitiveEncoder[Boolean].contramap(_.result)

  implicit val cqlColumnDecoderForMutationResultIsAmbiguous1: CqlPrimitiveDecoder[MutationResult] =
    CqlPrimitiveDecoder[Boolean].map(make)

  implicit val cqlColumnDecoderForMutationResultIsAmbiguous2: CqlPrimitiveDecoder[MutationResult] =
    CqlPrimitiveDecoder[Boolean].map(make)
}
