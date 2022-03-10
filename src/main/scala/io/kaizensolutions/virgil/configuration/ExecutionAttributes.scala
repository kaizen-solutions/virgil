package io.kaizensolutions.virgil.configuration

import com.datastax.oss.driver.api.core.cql.{BatchStatementBuilder, BoundStatementBuilder}

final case class ExecutionAttributes(
  pageSize: Option[Int] = None,
  executionProfile: Option[String] = None,
  consistencyLevel: Option[ConsistencyLevel] = None,
  idempotent: Option[Boolean] = None
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
      idempotent = self.idempotent.orElse(that.idempotent)
    )

  private[virgil] def configure(s: BoundStatementBuilder): BoundStatementBuilder = {
    val withPageSz = pageSize.fold(ifEmpty = s)(s.setPageSize)
    val withEP     = executionProfile.fold(ifEmpty = withPageSz)(withPageSz.setExecutionProfileName)
    val withCL     = consistencyLevel.fold(ifEmpty = withEP)(cl => withEP.setConsistencyLevel(cl.toDriver))
    val result     = idempotent.fold(ifEmpty = withCL)(withCL.setIdempotence(_))
    result
  }

  private[virgil] def configureBatch(s: BatchStatementBuilder): BatchStatementBuilder = {
    val withPageSz = pageSize.fold(ifEmpty = s)(s.setPageSize)
    val withEP     = executionProfile.fold(ifEmpty = withPageSz)(withPageSz.setExecutionProfileName)
    val withCL     = consistencyLevel.fold(ifEmpty = withEP)(cl => withEP.setConsistencyLevel(cl.toDriver))
    val result     = idempotent.fold(ifEmpty = withCL)(withCL.setIdempotence(_))
    result
  }
}
object ExecutionAttributes {
  def default: ExecutionAttributes = ExecutionAttributes()
}
