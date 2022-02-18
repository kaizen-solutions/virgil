package io.kaizensolutions.virgil.cqldsl.customcodecs

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{TypeCodec, TypeCodecs}
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import io.kaizensolutions.virgil.cqldsl.{CList, CassandraType}

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class CListCodec[A <: CassandraType](elementCodec: TypeCodec[A]) extends TypeCodec[CList[A]] {
  private final val listCodec = TypeCodecs.listOf(elementCodec)

  override def getJavaType: GenericType[CList[A]] = GenericType.of(classOf[CList[A]])

  override def getCqlType: DataType = DataTypes.listOf(elementCodec.getCqlType)

  override def encode(cList: CList[A], protocolVersion: ProtocolVersion): ByteBuffer =
    listCodec.encode(cList.value.asJava, protocolVersion)

  override def decode(bytes: ByteBuffer, protocolVersion: ProtocolVersion): CList[A] =
    CList(listCodec.decode(bytes, protocolVersion).asScala.toList)

  override def format(cList: CList[A]): String =
    listCodec.format(cList.value.asJava)

  override def parse(value: String): CList[A] =
    CList(listCodec.parse(value).asScala.toList)
}
