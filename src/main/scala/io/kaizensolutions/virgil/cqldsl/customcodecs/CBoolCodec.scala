package io.kaizensolutions.virgil.cqldsl.customcodecs

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import io.kaizensolutions.virgil.cqldsl.CBool

import java.nio.ByteBuffer

object CBoolCodec extends TypeCodec[CBool] {
  private final val boolCodec = TypeCodecs.BOOLEAN

  override def getJavaType: GenericType[CBool] = GenericType.of(classOf[CBool])

  override def getCqlType: DataType = DataTypes.BOOLEAN

  override def encode(cBool: CBool, protocolVersion: ProtocolVersion): ByteBuffer =
    boolCodec.encode(cBool.value, protocolVersion)

  override def decode(bytes: ByteBuffer, protocolVersion: ProtocolVersion): CBool =
    CBool(boolCodec.decode(bytes, protocolVersion))

  override def format(cBool: CBool): String =
    boolCodec.format(cBool.value)

  override def parse(value: String): CBool =
    CBool(boolCodec.parse(value))
}
