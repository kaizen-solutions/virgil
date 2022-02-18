package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.cql.{ResultSet, Row, SimpleStatement}
import com.datastax.oss.driver.api.core.data.GettableByName
import com.datastax.oss.driver.api.core.{CqlIdentifier, CqlSession}
import com.datastax.oss.protocol.internal.ProtocolConstants
import io.kaizensolutions.virgil.cqldsl._

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

object Test extends App {
  def decode(row: Row): CassandraType = {
    def iterate(gettableByName: GettableByName, field: String): CassandraType = {
      val dt = gettableByName.getType(field)

      if (dt.getProtocolCode == ProtocolConstants.DataType.UDT) {
        val udt = gettableByName.getUdtValue(field)
        val userdt = udt.getType
        val udtFields: List[String] = userdt.getFieldNames.asScala.toList.map(_.asInternal())

        CUdt(udtFields.zip(udtFields.map(iterate(udt, _))))
      }
      else {
        // be bad for now until we figure out what to do
        val codec = CassandraType.getCodecFor(dt).getOrElse(throw new Exception("type unsupported"))
        gettableByName.get(field, codec)
      }
    }

    val columns = row.getColumnDefinitions.iterator().asScala.toList.map(_.getName.asInternal())
    CUdt(columns.zip(columns.map(iterate(row, _))))
  }

  val session = CqlSession.builder.build
  val res: ResultSet =
    session
      .execute(SimpleStatement.newInstance("SELECT * FROM my_table WHERE id = 1")
      .setKeyspace(CqlIdentifier.fromCql("my_keyspace")))

  val row: Row = res.one()
  val ctype: DataType = row.getType("bob")

  val cdata: ByteBuffer = row.getBytesUnsafe("bob")

//  val udt = row.get


  row.get(CqlIdentifier.fromCql("blah"), classOf[String])
}
