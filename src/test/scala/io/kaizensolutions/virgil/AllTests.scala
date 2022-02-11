package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import io.kaizensolutions.virgil.cql._
import zio._
import zio.blocking.{effectBlocking, Blocking}
import zio.stream.ZStream
import zio.test._
import zio.test.environment.TestEnvironment

import java.net.InetSocketAddress

object AllTests extends DefaultRunnableSpec {
  val dependencies: URLayer[Blocking, Has[CQLExecutor]] = {
    val managedSession =
      for {
        c           <- CassandraContainer(CassandraType.Plain)
        details     <- (c.getHost).zip(c.getPort).toManaged_
        (host, port) = details
        session <- CQLExecutor(
                     CqlSession
                       .builder()
                       .withLocalDatacenter("dc1")
                       .addContactPoint(InetSocketAddress.createUnresolved(host, port))
                   )
        createKeyspace =
          cql"""CREATE KEYSPACE IF NOT EXISTS virgil
          WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
          }""".mutation
        useKeyspace = cql"USE virgil".mutation
        _          <- session.execute(createKeyspace).runDrain.toManaged_
        _          <- session.execute(useKeyspace).runDrain.toManaged_
        _          <- runMigration(session, "migrations.cql").toManaged_
      } yield session

    managedSession.toLayer.orDie
  }

  def runMigration(executor: CQLExecutor, fileName: String): ZIO[Blocking, Throwable, Unit] = {
    val migrationCql =
      ZStream
        .fromEffect(effectBlocking(scala.io.Source.fromResource(fileName).getLines()))
        .flatMap(ZStream.fromIterator(_))
        .map(_.strip())
        .filterNot { l =>
          l.isEmpty ||
          l.startsWith("--") ||
          l.startsWith("//")
        }
        .fold("")(_ ++ _)
        .map(_.split(";"))

    for {
      migrations <- migrationCql
      _          <- ZIO.foreach_(migrations)(str => executor.execute(str.asCql.mutation).runDrain)
    } yield ()
  }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("Virgil Test Suite") {
      (CQLExecutorSpec.sessionSpec + UserDefinedTypesSpec.userDefinedTypesSpec) @@ TestAspect.parallel
    }.provideCustomLayerShared(dependencies)
}
