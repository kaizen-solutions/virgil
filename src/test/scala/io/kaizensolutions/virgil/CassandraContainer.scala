package io.kaizensolutions.virgil

import com.dimafeng.testcontainers.GenericContainer
import zio._

trait CassandraContainer {
  def getHost: Task[String]
  def getPort: Task[Int]
}
object CassandraContainer {
  def apply(cassType: CassandraType): UManaged[CassandraContainer] = {
    val imageName  = "datastax/dse-server:6.8.19"
    val nativePort = 9042
    val env = Map(
      "DS_LICENSE"     -> "accept",
      "JVM_EXTRA_OPTS" -> "-Dcassandra.initial_token=0 -Dcassandra.skip_wait_for_gossip_to_settle=0"
    )

    val container = cassType match {
      case CassandraType.Plain =>
        GenericContainer(
          dockerImage = imageName,
          env = env,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Search =>
        GenericContainer(
          dockerImage = imageName,
          command = Seq("-s"),
          env = env,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Graph =>
        GenericContainer(
          dockerImage = imageName,
          command = Seq("-g"),
          env = env,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Analytics =>
        GenericContainer(
          dockerImage = imageName,
          command = Seq("-k"),
          env = env,
          exposedPorts = Seq(nativePort)
        )

      case CassandraType.Full =>
        GenericContainer(
          dockerImage = imageName,
          command = Seq("-s -k -g"),
          env = env,
          exposedPorts = Seq(nativePort)
        )
    }

    ZManaged
      .make_(ZIO.effectTotal(container.start()))(ZIO.effectTotal(container.stop()))
      .as(
        new CassandraContainer {
          override def getHost: Task[String] = Task(container.host)
          override def getPort: Task[Int]    = Task(container.mappedPort(9042))
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
