package io.kaizensolutions.virgil.configuration

import com.datastax.oss.driver.api.core.cql.PagingState

final case class PageState(private[virgil] val underlying: PagingState) {
  def toBytes: Array[Byte]      = underlying.toBytes
  override def toString: String = underlying.toString
}
object PageState {
  def fromDriver(pagingState: PagingState): PageState = PageState(pagingState)
  def fromBytes(in: Array[Byte]): PageState           = PageState(PagingState.fromBytes(in))
  def fromString(in: String): PageState               = PageState(PagingState.fromString(in))
}
