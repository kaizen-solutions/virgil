package io.kaizensolutions.virgil

sealed trait BatchType
object BatchType {
  case object Logged   extends BatchType
  case object Unlogged extends BatchType
  case object Counter  extends BatchType
}
