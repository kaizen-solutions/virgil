package io.kaizensolutions.virgil

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.dimafeng.testcontainers.GenericContainer

trait CassandraContainer {
  def getHost: IO[String]
  def getPort: IO[Int]
}

object CassandraContainer {
  def apply(cassType: CassandraType): Resource[IO, CassandraContainer] = {
    val nativePort         = 9042
    val datastaxEnterprise = "datastax/dse-server:6.9.1"
    val datastaxEnv = Map(
      "DS_LICENSE"     -> "accept",
      "JVM_EXTRA_OPTS" -> "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1 -Dcassandra.num_tokens=nil -Dcassandra.allocate_tokens_for_local_replication_factor=nil"
    )
    val vanilla = "cassandra:5"
    val vanillaEnv = Map(
      "CASSANDRA_ENDPOINT_SNITCH" -> "GossipingPropertyFileSnitch",
      "CASSANDRA_DC"              -> "dc1",
      "CASSANDRA_NUM_TOKENS"      -> "1",
      "CASSANDRA_START_RPC"       -> "false",
      "JVM_EXTRA_OPTS"            -> datastaxEnv("JVM_EXTRA_OPTS")
    )

    val container = cassType match {
      case CassandraType.Plain =>
        GenericContainer(
          dockerImage = vanilla,
          env = vanillaEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Search =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-s"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Graph =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-g"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Analytics =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-k"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Full =>
        GenericContainer(
          dockerImage = datastaxEnterprise,
          command = Seq("-s -k -g"),
          env = datastaxEnv,
          exposedPorts = Seq(nativePort)
        )
    }

    Resource
      .make(IO.delay(container.start()))(_ => IO.delay(container.stop()))
      .as {
        new CassandraContainer {
          override def getHost: IO[String] = IO.fromEither(Either.catchNonFatal(container.host))
          override def getPort: IO[Int]    = IO.fromEither(Either.catchNonFatal(container.mappedPort(nativePort)))
        }
      }

  }
}

sealed trait CassandraType
object CassandraType {
  case object Plain     extends CassandraType
  case object Search    extends CassandraType
  case object Graph     extends CassandraType
  case object Analytics extends CassandraType
  case object Full      extends CassandraType
}
