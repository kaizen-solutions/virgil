package io.kaizensolutions.virgil.internal

import cats.effect._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchType => _}
import com.datastax.oss.driver.api.core.metrics.Metrics
import fs2._
import io.kaizensolutions.virgil._
import io.kaizensolutions.virgil.configuration.PageState
import io.kaizensolutions.virgil.internal.Proofs._

import scala.annotation.nowarn

/**
 * CQLExecutorImpl is a ZIO based client for the Apache Cassandra Java Driver
 * that provides ZIO and ZStream abstractions over the Datastax Java driver. We
 * consider CQLExecutor to be the interpreter of [[CQL[A]]] expressions.
 *
 * @param underlyingSession
 *   is the underlying Datastax Java driver session
 */
@nowarn // TODO: Remove
private[virgil] class CQLExecutorImpl[F[_]](underlyingSession: CqlSession)(implicit F: Async[F])
    extends CQLExecutor[F] {
  override def execute[A](in: CQL[A]): Stream[F, A] = ???

  override def executeMutation(in: CQL[MutationResult]): F[MutationResult] = ???

  override def executePage[A](in: CQL[A], pageState: Option[PageState])(implicit
    ev: A =:!= MutationResult
  ): F[Paged[A]] = ???

  override def metrics: F[Option[Metrics]] = ???
}
