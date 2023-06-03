package io.kaizensolutions.virgil

import cats.Show
import cats.effect._
import cats.syntax.all._
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import fs2.io.file._
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.models._
import weaver._

import java.net.InetSocketAddress

trait ResourceSuite {

  implicit val resourceTag: ResourceTag[CQLExecutor[IO]]                     = ResourceTag.classBasedInstance[CQLExecutor[IO]]
  implicit val RowPersonShow: Show[UserDefinedTypesSpecDatatypes.Row_Person] = Show.fromToString
  implicit val RowHeavilyNestedUDTTableShow: Show[UserDefinedTypesSpecDatatypes.Row_HeavilyNestedUDTTable] =
    Show.fromToString
  implicit val UpdateBuilderSpecPersonShow: Show[UpdateBuilderSpecDatatypes.UpdateBuilderSpecPerson] = Show.fromToString
  implicit val UpdateBuilderSpecCounterShow: Show[UpdateBuilderSpecDatatypes.UpdateBuilderSpecCounter] =
    Show.fromToString
  implicit val RelationSpecPersonShow: Show[RelationSpecDatatypes.RelationSpec_Person] = Show.fromToString
  implicit val DeleteBuilderSpecPersonShow: Show[DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person] =
    Show.fromToString
  implicit val InsertBuilderSpecPersonShow: Show[InsertBuilderSpecDatatypes.InsertBuilderSpecPerson] =
    Show.fromToString
  implicit val CursorExampleRowShow: Show[CursorSpecDatatypes.CursorExampleRow]            = Show.fromToString
  implicit val SimpleCollectionRowShow: Show[CollectionsSpecDatatypes.SimpleCollectionRow] = Show.fromToString
  implicit val NestedCollectionRowShow: Show[CollectionsSpecDatatypes.NestedCollectionRow] = Show.fromToString
  implicit val OptionCollectionRowShow: Show[CollectionsSpecDatatypes.OptionCollectionRow] = Show.fromToString

  implicit val SelectPageRowShow: Show[CqlExecutorSpecDatatypes.SelectPageRow]       = Show.fromToString
  implicit val ExecuteTestTableShow: Show[CqlExecutorSpecDatatypes.ExecuteTestTable] = Show.fromToString
  implicit val TimeoutCheckRowShow: Show[CqlExecutorSpecDatatypes.TimeoutCheckRow]   = Show.fromToString

  def runMigration(cql: CQLExecutor[IO], fileName: String): Resource[IO, Unit] = {
    val migrationCql = Files[IO]
      .readUtf8Lines(Path(getClass().getClassLoader().getResource(fileName).getPath()))
      .map(_.strip())
      .filterNot { l =>
        l.isEmpty ||
        l.startsWith("--") ||
        l.startsWith("//")
      }
      .fold("")(_ ++ _)
      .map(_.split(";"))

    for {
      migrations <- Resource.eval(migrationCql.compile.lastOrError)
      _ <- Resource.eval(
             migrations.toList
               .traverse(str => cql.execute(str.asCql.mutation).compile.drain)
               .void
           )
    } yield ()
  }

}

object SharedResources extends ResourceSuite with GlobalResource {

  override def sharedResources(global: GlobalWrite): Resource[IO, Unit] =
    for {
      cassandra <- CassandraContainer(CassandraType.Plain)
      host      <- Resource.eval(cassandra.getHost)
      port      <- Resource.eval(cassandra.getPort)
      session <- CQLExecutor[IO](
                   CqlSession
                     .builder()
                     .withLocalDatacenter("dc1")
                     .addContactPoint(InetSocketAddress.createUnresolved(host, port))
                     .withApplicationName("virgil-weaver-test")
                     .withConfigLoader(
                       DriverConfigLoader
                         .programmaticBuilder()
                         .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(10))
                         .build()
                     )
                 )
      createKeyspace =
        cql"""CREATE KEYSPACE IF NOT EXISTS virgil
          WITH REPLICATION = {
            'class': 'SimpleStrategy',
            'replication_factor': 1
          }""".mutation
      useKeyspace = cql"USE virgil".mutation
      _          <- Resource.eval(session.execute(createKeyspace).compile.drain)
      _          <- Resource.eval(session.execute(useKeyspace).compile.drain)
      _          <- runMigration(session, "migrations.cql")
      _          <- global.putR(session)
    } yield ()
}
