package io.kaizensolutions.virgil.cqldsl.customcodecs

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import io.kaizensolutions.virgil.cqldsl.CInt

import java.nio.ByteBuffer

object CIntCodec extends TypeCodec[CInt] {
  private final val intCodec = TypeCodecs.INT

  override def getJavaType: GenericType[CInt] = GenericType.of(classOf[CInt])

  override def getCqlType: DataType = DataTypes.INT

  override def encode(cInt: CInt, protocolVersion: ProtocolVersion): ByteBuffer =
    intCodec.encode(cInt.value, protocolVersion)

  override def decode(bytes: ByteBuffer, protocolVersion: ProtocolVersion): CInt =
    CInt(intCodec.decode(bytes, protocolVersion))

  override def format(cInt: CInt): String =
    intCodec.format(cInt.value)

  override def parse(value: String): CInt =
    CInt(intCodec.parse(value))
}
