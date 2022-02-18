package io.kaizensolutions.virgil.cqldsl

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodec
import com.datastax.oss.protocol.internal.ProtocolConstants
import io.kaizensolutions.virgil.cqldsl.customcodecs.CIntCodec

sealed trait CassandraType extends Product with Serializable

case object CNull extends CassandraType

sealed trait CPrimative extends CassandraType
case class CInt(value: Int) extends CPrimative

case class CUdt(values: List[(String, CassandraType)]) extends CassandraType

object CassandraType {
  def getCodecFor(dt: DataType): Option[TypeCodec[_ <: CassandraType]] = dt.getProtocolCode match {
    case ProtocolConstants.DataType.CUSTOM    => None
    case ProtocolConstants.DataType.ASCII     => None
    case ProtocolConstants.DataType.BIGINT    => None
    case ProtocolConstants.DataType.BLOB      => None
    case ProtocolConstants.DataType.BOOLEAN   => None
    case ProtocolConstants.DataType.COUNTER   => None
    case ProtocolConstants.DataType.DECIMAL   => None
    case ProtocolConstants.DataType.DOUBLE    => None
    case ProtocolConstants.DataType.FLOAT     => None
    case ProtocolConstants.DataType.INT       => Some(CIntCodec)
    case ProtocolConstants.DataType.TIMESTAMP => None
    case ProtocolConstants.DataType.UUID      => None
    case ProtocolConstants.DataType.VARCHAR   => None
    case ProtocolConstants.DataType.VARINT    => None
    case ProtocolConstants.DataType.TIMEUUID  => None
    case ProtocolConstants.DataType.INET      => None
    case ProtocolConstants.DataType.DATE      => None
    case ProtocolConstants.DataType.TIME      => None
    case ProtocolConstants.DataType.SMALLINT  => None
    case ProtocolConstants.DataType.TINYINT   => None
    case ProtocolConstants.DataType.DURATION  => None
    case ProtocolConstants.DataType.LIST      => None
    case ProtocolConstants.DataType.MAP       => None
    case ProtocolConstants.DataType.SET       => None
    case ProtocolConstants.DataType.UDT       => None
    case ProtocolConstants.DataType.TUPLE     => None
  }
}

