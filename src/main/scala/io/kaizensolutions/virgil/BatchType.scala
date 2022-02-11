package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.DefaultBatchType

sealed trait BatchType { self =>
  def combine(that: BatchType): BatchType =
    (self, that) match {
      case (BatchType.Logged, _) | (_, BatchType.Logged)     => BatchType.Logged
      case (BatchType.Unlogged, _) | (_, BatchType.Unlogged) => BatchType.Unlogged
      case (BatchType.Counter, BatchType.Counter)            => BatchType.Counter
    }

  private[virgil] def toDriver: DefaultBatchType =
    self match {
      case BatchType.Logged   => DefaultBatchType.LOGGED
      case BatchType.Unlogged => DefaultBatchType.UNLOGGED
      case BatchType.Counter  => DefaultBatchType.COUNTER
    }
}
object BatchType {
  case object Logged   extends BatchType
  case object Unlogged extends BatchType
  case object Counter  extends BatchType
}
