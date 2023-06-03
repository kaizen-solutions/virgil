package io.kaizensolutions.virgil.configuration

import com.datastax.oss.driver.api.core.cql._

import java.time.Duration
import scala.math.Ordering.Implicits._

final case class ExecutionAttributes(
  pageSize: Option[Int] = None,
  executionProfile: Option[String] = None,
  consistencyLevel: Option[ConsistencyLevel] = None,
  idempotent: Option[Boolean] = None,
  timeout: Option[Duration] = None
) { self =>
  def debug: String =
    s"ExecutionAttributes(pageSize = $pageSize, executionProfile = $executionProfile, consistencyLevel = $consistencyLevel, idempotent = $idempotent)"

  def withPageSize(pageSize: Int): ExecutionAttributes =
    copy(pageSize = Some(pageSize))

  def withExecutionProfile(executionProfile: String): ExecutionAttributes =
    copy(executionProfile = Some(executionProfile))

  def withConsistencyLevel(consistencyLevel: ConsistencyLevel): ExecutionAttributes =
    copy(consistencyLevel = Some(consistencyLevel));

  def withIdempotent(idempotent: Boolean): ExecutionAttributes =
    copy(idempotent = Some(idempotent))

  def withTimeout(timeout: Duration): ExecutionAttributes =
    copy(timeout = Some(timeout))

  def combine(that: ExecutionAttributes): ExecutionAttributes =
    ExecutionAttributes(
      pageSize = (self.pageSize, that.pageSize) match {
        case (Some(a), Some(b)) =>
          if (a > b) Some(a)
          else Some(b)

        case (Some(a), None) => Some(a)
        case (None, Some(b)) => Some(b)
        case (None, None)    => None
      },
      executionProfile = self.executionProfile.orElse(that.executionProfile),
      consistencyLevel = (self.consistencyLevel, that.consistencyLevel) match {
        case (Some(a), Some(_)) => Some(a)
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      },
      idempotent = self.idempotent.orElse(that.idempotent),
      timeout = (self.timeout, that.timeout) match {
        case (Some(a), Some(b)) => Some(a.max(b))
        case (Some(a), None)    => Some(a)
        case (None, Some(b))    => Some(b)
        case (None, None)       => None
      }
    )

  private[virgil] def configure(s: BoundStatementBuilder): BoundStatementBuilder =
    config[BoundStatement, BoundStatementBuilder](s)

  private[virgil] def configureBatch(b: BatchStatementBuilder): BatchStatementBuilder =
    config[BatchStatement, BatchStatementBuilder](b)

  private def config[SType <: Statement[SType], Builder <: StatementBuilder[Builder, SType]](
    s: Builder
  ): Builder = {
    val withPageSz = pageSize.fold(ifEmpty = s)(s.setPageSize)
    val withEP     = executionProfile.fold(ifEmpty = withPageSz)(withPageSz.setExecutionProfileName)
    val withCL     = consistencyLevel.fold(ifEmpty = withEP)(cl => withEP.setConsistencyLevel(cl.toDriver))
    val withIdem   = idempotent.fold(ifEmpty = withCL)(withCL.setIdempotence(_))
    val withTime   = timeout.fold(ifEmpty = withIdem)(withIdem.setTimeout)
    withTime
  }
}
object ExecutionAttributes {
  def default: ExecutionAttributes = ExecutionAttributes()
}
