package io.kaizensolutions.virgil.configuration

import com.datastax.oss.driver.api.core.ConsistencyLevel

sealed trait CassandraConsistencyLevel { self =>
  def toDriver: ConsistencyLevel = self match {
    case CassandraConsistencyLevel.One         => ConsistencyLevel.ONE
    case CassandraConsistencyLevel.Two         => ConsistencyLevel.TWO
    case CassandraConsistencyLevel.Three       => ConsistencyLevel.THREE
    case CassandraConsistencyLevel.Quorum      => ConsistencyLevel.QUORUM
    case CassandraConsistencyLevel.All         => ConsistencyLevel.ALL
    case CassandraConsistencyLevel.LocalOne    => ConsistencyLevel.LOCAL_ONE
    case CassandraConsistencyLevel.EachQuorum  => ConsistencyLevel.EACH_QUORUM
    case CassandraConsistencyLevel.LocalQuorum => ConsistencyLevel.LOCAL_QUORUM
    case CassandraConsistencyLevel.Serial      => ConsistencyLevel.SERIAL
    case CassandraConsistencyLevel.LocalSerial => ConsistencyLevel.LOCAL_SERIAL
  }
}
object CassandraConsistencyLevel {
  case object One         extends CassandraConsistencyLevel
  case object Two         extends CassandraConsistencyLevel
  case object Three       extends CassandraConsistencyLevel
  case object Quorum      extends CassandraConsistencyLevel
  case object All         extends CassandraConsistencyLevel
  case object LocalOne    extends CassandraConsistencyLevel
  case object EachQuorum  extends CassandraConsistencyLevel
  case object LocalQuorum extends CassandraConsistencyLevel
  case object Serial      extends CassandraConsistencyLevel
  case object LocalSerial extends CassandraConsistencyLevel
}
