package io.kaizensolutions.virgil

sealed trait PullMode
object PullMode {
  type All = All.type
  case object All              extends PullMode
  case class TakeUpto(n: Long) extends PullMode
}
