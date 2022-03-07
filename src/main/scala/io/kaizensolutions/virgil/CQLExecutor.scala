package io.kaizensolutions.virgil
import com.datastax.oss.driver.api.core.{CqlSession, CqlSessionBuilder}
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.CQLExecutorImpl
import io.kaizensolutions.virgil.internal.Proofs.=:!=
import zio.stream._
import zio.{Has, RIO, RLayer, Task, TaskManaged, URLayer, ZIO, ZLayer, ZManaged}

trait CQLExecutor {
  def execute[A](in: CQL[A]): Stream[Throwable, A]

  def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit ev: A =:!= MutationResult): Task[Paged[A]]
}
object CQLExecutor {
  def execute[A](in: CQL[A]): ZStream[Has[CQLExecutor], Throwable, A] =
    ZStream.serviceWithStream(_.execute(in))

  def executePage[A](in: CQL[A], pageState: Option[PageState] = None)(implicit
    ev: A =:!= MutationResult
  ): RIO[Has[CQLExecutor], Paged[A]] = ZIO.serviceWith[CQLExecutor](_.executePage(in, pageState))

  val live: RLayer[Has[CqlSessionBuilder], Has[CQLExecutor]] =
    ZLayer.fromServiceManaged[CqlSessionBuilder, Any, Throwable, CQLExecutor](apply)

  val sessionLive: URLayer[Has[CqlSession], Has[CQLExecutor]] =
    ZLayer.fromService[CqlSession, CQLExecutor](fromCqlSession)

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

  def apply(builder: CqlSessionBuilder): TaskManaged[CQLExecutor] = {
    val acquire = Task.effect(builder.build())
    val release = (session: CqlSession) => ZIO(session.close()).ignore

    ZManaged
      .make(acquire)(release)
      .map(new CQLExecutorImpl(_))
  }
}
