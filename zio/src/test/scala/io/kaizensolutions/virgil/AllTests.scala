package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core._
import io.kaizensolutions.virgil.cql._
import zio._
import zio.logging.backend.SLF4J
import zio.stream.ZStream
import zio.test.TestAspect._
import zio.test._

import java.net.InetSocketAddress

object AllTests extends ZIOSpecDefault {
  override val bootstrap: ULayer[TestEnvironment] = Runtime.removeDefaultLoggers ++ SLF4J.slf4j ++ testEnvironment

  override def spec =
    suite("Virgil Test Suite") {
      CQLExecutorSpec.executorSpec +
        UserDefinedTypesSpec.userDefinedTypesSpec +
        CollectionsSpec.collectionsSpec +
        CursorSpec.cursorSpec +
        UpdateBuilderSpec.updateBuilderSpec +
        RelationSpec.relationSpec +
        DeleteBuilderSpec.deleteBuilderSpec +
        InsertBuilderSpec.insertBuilderSpec +
        SelectBuilderSpec.selectBuilderSpec
    }.provideSomeShared[TestEnvironment](containerLayer, executorLayer, keyspaceAndMigrations) @@ sequential @@ timed

  val keyspaceAndMigrations =
    ZLayer {
      for {
        executor      <- ZIO.service[CQLExecutor]
        createKeyspace =
          cql"""CREATE KEYSPACE IF NOT EXISTS virgil
          WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
          }""".mutation
        useKeyspace = cql"USE virgil".mutation
        _          <- executor.execute(createKeyspace).runDrain
        _          <- executor.execute(useKeyspace).runDrain
        _          <- runMigration(executor, "migrations.cql")
      } yield ()
    }

  val containerLayer: ULayer[CassandraContainer] = ZLayer.scoped(CassandraContainer(CassandraType.Plain))

  val executorLayer: RLayer[CassandraContainer, CqlSessionBuilder & CQLExecutor] =
    ZLayer.scopedEnvironment[CassandraContainer](
      for {
        container   <- ZIO.service[CassandraContainer]
        hostAndPort <- container.getHost.zip(container.getPort)
        (host, port) = hostAndPort
        builder     <- ZIO.attempt(
                     CqlSession
                       .builder()
                       .withLocalDatacenter("dc1")
                       .addContactPoint(InetSocketAddress.createUnresolved(host, port))
                   )
        executor <- CQLExecutor(builder)
      } yield ZEnvironment.empty.add(builder).add(executor)
    )

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
}
