package io.kaizensolutions.virgil

import com.dimafeng.testcontainers.GenericContainer
import zio._

trait CassandraContainer {
  def getHost: Task[String]
  def getPort: Task[Int]
}
object CassandraContainer {
  def apply(cassType: CassandraType): URIO[Scope, CassandraContainer] = {
    val nativePort         = 9042
    val datastaxEnterprise = "datastax/dse-server:6.9.1"
    val datastaxEnv = Map(
      "DS_LICENSE"     -> "accept",
      "JVM_EXTRA_OPTS" -> "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1 -Dcassandra.num_tokens=nil -Dcassandra.allocate_tokens_for_local_replication_factor=nil -D.cassandra.auto_snapshot=false -Dcassandra.force_default_indexing_page_size=4096"
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

    ZIO
      .acquireRelease(ZIO.succeed(container.start()))(_ => ZIO.succeed(container.stop()))
      .as(
        new CassandraContainer {
          override def getHost: Task[String] = ZIO.attempt(container.host)
          override def getPort: Task[Int]    = ZIO.attempt(container.mappedPort(9042))
        }
      )
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
