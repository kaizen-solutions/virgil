package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.CqlSession
import io.kaizensolutions.virgil.cql._
import zio._
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.test._

import java.net.InetSocketAddress

object AllTests extends ZIOSpecDefault {
  val containerLayer: ULayer[CassandraContainer] = ZLayer.scoped(CassandraContainer(CassandraType.Plain))
  val executorLayer: URLayer[CassandraContainer, CQLExecutor] = {
    val keyspaceAndMigrations =
      for {
        c           <- ZIO.service[CassandraContainer]
        details     <- c.getHost.zip(c.getPort)
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
        _          <- session.execute(createKeyspace).runDrain
        _          <- session.execute(useKeyspace).runDrain
        _          <- runMigration(session, "migrations.cql")
      } yield session
    val sessionLayer: URLayer[CassandraContainer, CQLExecutor] = ZLayer.scoped(keyspaceAndMigrations).orDie
    sessionLayer
  }

  def runMigration(cql: CQLExecutor, fileName: String): Task[Unit] = {
    val migrationCql =
      ZStream
        .fromZIO(ZIO.attemptBlocking(scala.io.Source.fromResource(fileName).getLines()))
        .flatMap(ZStream.fromIterator(_))
        .map(_.strip())
        .filterNot { l =>
          l.isEmpty ||
          l.startsWith("--") ||
          l.startsWith("//")
        }
        .runFold("")(_ ++ _)
        .map(_.split(";"))

    for {
      migrations <- migrationCql
      _          <- ZIO.foreachDiscard(migrations)(str => cql.execute(str.asCql.mutation).runDrain)
    } yield ()
  }

  override def spec =
    suite("Virgil Test Suite") {
        CQLExecutorSpec.executorSpec +
        UserDefinedTypesSpec.userDefinedTypesSpec +
        CollectionsSpec.collectionsSpec +
        CursorSpec.cursorSpec +
        UpdateBuilderSpec.updateBuilderSpec +
        RelationSpec.relationSpec +
        DeleteBuilderSpec.deleteBuilderSpec +
        InsertBuilderSpec.insertBuilderSpec
    }.provideSomeShared[TestEnvironment](containerLayer, executorLayer) @@ parallel @@ timed
}
