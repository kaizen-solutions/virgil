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
  val dependencies: URLayer[Blocking, Has[ZioCassandraSession]] = {
    val managedSession =
      for {
        c           <- CassandraContainer(CassandraType.Plain)
        details     <- (c.getHost).zip(c.getPort).toManaged_
        (host, port) = details
        session <- ZioCassandraSession(
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
          }""".action
        useKeyspace = cql"USE virgil".action
        _          <- session.execute(createKeyspace).toManaged_
        _          <- session.execute(useKeyspace).toManaged_
        _          <- runMigration(session, "migrations.cql").toManaged_
      } yield session

    managedSession.toLayer.orDie
  }

  def runMigration(session: ZioCassandraSession, fileName: String): ZIO[Blocking, Throwable, Unit] = {
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
      _          <- ZIO.foreach_(migrations)(session.executeAction)
    } yield ()
  }

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("Virgil Test Suite") {
      (ZioCassandraSessionSpec.sessionSpec + UserDefinedTypesSpec.userDefinedTypesSpec) @@ TestAspect.parallel
    }.provideCustomLayerShared(dependencies)
}
