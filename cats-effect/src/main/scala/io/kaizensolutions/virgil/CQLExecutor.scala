package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.metrics.Metrics
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.Proofs.=:!=
import cats.effect._
import fs2._
import io.kaizensolutions.virgil.internal.CQLExecutorImpl

trait CQLExecutor[F[_]] {
  def execute[A](in: CQL[A]): Stream[F, A]

  def executeMutation(in: CQL[MutationResult]): F[MutationResult]

  def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit
    ev: A =:!= MutationResult
  ): F[Paged[A]]

  def metrics: F[Option[Metrics]]
}
object CQLExecutor {

  /**
   * Create a CQL Executor from an existing Datastax Java Driver's CqlSession
   * Note that the user is responsible for the lifecycle of the underlying
   * CqlSession
   * @param session
   *   is the underlying Datastax Java Driver's CqlSession
   * @return
   *   the CQLExecutor
   */
  def fromCqlSession[F[_]: Async](session: CqlSession): CQLExecutor[F] =
    new CQLExecutorImpl[F](session)

  def apply[F[_]](builder: => CqlSessionBuilder)(implicit F: Async[F]): Resource[F, CQLExecutor[F]] = {
    val acquire: F[CqlSession]         = F.delay(builder.build())
    val release: CqlSession => F[Unit] = (session: CqlSession) => F.delay(session.close())
    Resource
      .make[F, CqlSession](acquire)(release)
      .map(new CQLExecutorImpl[F](_))
  }
}
