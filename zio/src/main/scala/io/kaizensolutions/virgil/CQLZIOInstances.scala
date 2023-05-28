package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.configuration.PageState
import zio._
import zio.stream.ZStream

trait CQLZIOInstances {
  implicit class CQLZioOperations[+Result](self: CQL[Result]) {
    def execute(implicit trace: Trace): ZStream[CQLExecutor, Throwable, Result] = CQLExecutor.execute(self)

    def executeMutation(implicit ev: Result <:< MutationResult, trace: Trace): RIO[CQLExecutor, MutationResult] =
      CQLExecutor.executeMutation(self.widen[MutationResult])

    def executePage[Result1 >: Result](state: Option[PageState] = None)(implicit
      trace: Trace
    ): RIO[CQLExecutor, Paged[Result1]] =
      CQLExecutor.executePage(self, state)
  }
}
object CQLZIOInstances extends CQLZIOInstances
