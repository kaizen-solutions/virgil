package io.kaizensolutions.virgil
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.CQLExecutorImpl
import io.kaizensolutions.virgil.internal.Proofs.=:!=
import zio._
import zio.stream._

trait CQLExecutor {
  def execute[A](in: CQL[A]): Stream[Throwable, A]

  def executeMutation(in: CQL[MutationResult]): Task[MutationResult]

  def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit ev: A =:!= MutationResult): Task[Paged[A]]
}
object CQLExecutor {
  def execute[A](in: CQL[A]): ZStream[CQLExecutor, Throwable, A] =
    ZStream.serviceWithStream(_.execute(in))

  def executeMutation(in: CQL[MutationResult]): RIO[CQLExecutor, MutationResult] =
    ZIO.serviceWithZIO(_.executeMutation(in))

  def executePage[A](in: CQL[A], pageState: Option[PageState] = None)(implicit
    ev: A =:!= MutationResult
  ): RIO[CQLExecutor, Paged[A]] = ZIO.serviceWithZIO[CQLExecutor](_.executePage(in, pageState))

  val live: RLayer[CqlSessionBuilder, CQLExecutor] =
    ZLayer.scoped(
      for {
        builder  <- ZIO.service[CqlSessionBuilder]
        executor <- apply(builder)
      } yield executor
    )

  val sessionLive: URLayer[CqlSession, CQLExecutor] =
    ZLayer.fromFunction(fromCqlSession(_))

  /**
   * Create a CQL Executor from an existing Datastax Java Driver's CqlSession
   * Note that the user is responsible for the lifecycle of the underlying
   * CqlSession
   * @param session
   *   is the underlying Datastax Java Driver's CqlSession
   * @return
   *   the ZIO Cassandra Session
   */
  def fromCqlSession(session: CqlSession): CQLExecutor =
    new CQLExecutorImpl(session)

  def apply(builder: CqlSessionBuilder): RIO[Scope, CQLExecutor] = {
    val acquire = ZIO.attempt(builder.build())
    val release = (session: CqlSession) => ZIO.attempt(session.close()).ignore

    ZIO
      .acquireRelease(acquire)(release)
      .map(new CQLExecutorImpl(_))
  }
}
