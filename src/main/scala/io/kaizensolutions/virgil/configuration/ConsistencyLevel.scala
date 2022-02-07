package io.kaizensolutions.virgil.configuration

import com.datastax.oss.driver.api.core.{ConsistencyLevel => DriverConsistencyLevel}

sealed trait ConsistencyLevel { self =>
  def toDriver: DriverConsistencyLevel = self match {
    case ConsistencyLevel.One         => DriverConsistencyLevel.ONE
    case ConsistencyLevel.Two         => DriverConsistencyLevel.TWO
    case ConsistencyLevel.Three       => DriverConsistencyLevel.THREE
    case ConsistencyLevel.Quorum      => DriverConsistencyLevel.QUORUM
    case ConsistencyLevel.All         => DriverConsistencyLevel.ALL
    case ConsistencyLevel.LocalOne    => DriverConsistencyLevel.LOCAL_ONE
    case ConsistencyLevel.EachQuorum  => DriverConsistencyLevel.EACH_QUORUM
    case ConsistencyLevel.LocalQuorum => DriverConsistencyLevel.LOCAL_QUORUM
    case ConsistencyLevel.Serial      => DriverConsistencyLevel.SERIAL
    case ConsistencyLevel.LocalSerial => DriverConsistencyLevel.LOCAL_SERIAL
  }
}
object ConsistencyLevel {
  case object One         extends ConsistencyLevel
  case object Two         extends ConsistencyLevel
  case object Three       extends ConsistencyLevel
  case object Quorum      extends ConsistencyLevel
  case object All         extends ConsistencyLevel
  case object LocalOne    extends ConsistencyLevel
  case object EachQuorum  extends ConsistencyLevel
  case object LocalQuorum extends ConsistencyLevel
  case object Serial      extends ConsistencyLevel
  case object LocalSerial extends ConsistencyLevel
}
