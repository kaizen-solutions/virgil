package io.kaizensolutions.virgil.internal

sealed private[virgil] trait PullMode
object PullMode {
  type All = All.type
  private[virgil] case object All              extends PullMode
  private[virgil] case class TakeUpto(n: Long) extends PullMode
}
