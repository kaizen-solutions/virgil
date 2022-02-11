package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.{Reader, Writer}

final case class MutationResult private (result: Boolean) extends AnyVal
object MutationResult {
  def make(result: Boolean): MutationResult = MutationResult(result)

  // Make it such that you cannot accidentally create a Query of a MutationResult because this is an invalid state
  implicit val writerAmbiguous1: Writer[MutationResult] =
    Writer.make((b, _, _) => b)

  implicit val writerAmbiguous2: Writer[MutationResult] =
    Writer.make((b, _, _) => b)

  implicit val readerAmbiguous1: Reader[MutationResult] =
    Reader.make((_, _) => MutationResult(true))

  implicit val readerAmbiguous2: Reader[MutationResult] =
    Reader.make((_, _) => MutationResult(true))
}
