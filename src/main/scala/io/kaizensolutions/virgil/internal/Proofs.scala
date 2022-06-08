package io.kaizensolutions.virgil.internal

import scala.annotation.implicitNotFound

/**
 * Credits to Shapeless
 */
object Proofs {
  def unexpected: Nothing = sys.error("Unexpected invocation")

  @implicitNotFound("Cannot prove that ${A} is not the same type as ${B}")
  trait =:!=[A, B]
  implicit def neq[A, B]: A =:!= B        = new =:!=[A, B] {}
  implicit def neqAmbiguous1[A]: A =:!= A = unexpected
  implicit def neqAmbiguous2[A]: A =:!= A = unexpected

  @implicitNotFound("${A} must not be a subtype of ${B}")
  trait <:!<[A, B]
  implicit def nsub[A, B]: A <:!< B            = new <:!<[A, B] {}
  implicit def nsubAmbig1[A, B >: A]: A <:!< B = unexpected
  implicit def nsubAmbig2[A, B >: A]: A <:!< B = unexpected
}
