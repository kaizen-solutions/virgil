package io.kaizensolutions.virgil.dsl

import zio.NonEmptyChunk

sealed trait InsertConditions extends Conditions
sealed trait UpdateConditions extends Conditions
sealed trait DeleteConditions extends Conditions

sealed trait Conditions
object Conditions {
  case object NoConditions                                           extends InsertConditions with UpdateConditions with DeleteConditions
  case object IfExists                                               extends UpdateConditions with DeleteConditions
  case object IfNotExists                                            extends InsertConditions
  final case class IfConditions(conditions: NonEmptyChunk[Relation]) extends UpdateConditions with DeleteConditions
}
