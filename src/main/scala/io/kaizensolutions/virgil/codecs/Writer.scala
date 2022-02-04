package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

trait Writer[ScalaType] { self =>
  def write(builder: BoundStatementBuilder, column: String, value: ScalaType): BoundStatementBuilder

  def contramap[ScalaType2](f: ScalaType2 => ScalaType): Writer[ScalaType2] =
    (boundStatement: BoundStatementBuilder, column: String, value: ScalaType2) =>
      self.write(boundStatement, column, f(value))
}

object Writer extends TypeMapperSupport with MagnoliaWriterSupport {
  def apply[A](implicit ev: Writer[A]): Writer[A] = ev

  def make[ScalaType](f: (BoundStatementBuilder, String, ScalaType) => BoundStatementBuilder): Writer[ScalaType] =
    (boundStatement: BoundStatementBuilder, column: String, value: ScalaType) => f(boundStatement, column, value)

  implicit val bigDecimalWriter: Writer[BigDecimal] =
    make((bs, colName, scalaType) => bs.setBigDecimal(colName, scalaType.bigDecimal))
  implicit val bigIntWriter: Writer[BigInt] =
    make((bs, colName, scalaType) => bs.setBigInteger(colName, scalaType.bigInteger))
  implicit val booleanWriter: Writer[Boolean] =
    make((bs, colName, scalaType) => bs.setBoolean(colName, scalaType))
  implicit val byteBufferWriter: Writer[java.nio.ByteBuffer] =
    make((bs, colName, scalaType) => bs.setByteBuffer(colName, scalaType))
  implicit val byteWriter: Writer[Byte]     = make((bs, colName, scalaType) => bs.setByte(colName, scalaType))
  implicit val doubleWriter: Writer[Double] = make((bs, colName, scalaType) => bs.setDouble(colName, scalaType))
  implicit val instantWriter: Writer[java.time.Instant] =
    make((bs, colName, scalaType) => bs.setInstant(colName, scalaType))
  implicit val intWriter: Writer[Int] = make((bs, colName, scalaType) => bs.setInt(colName, scalaType))
  implicit val localDateWriter: Writer[java.time.LocalDate] =
    make((bs, colName, scalaType) => bs.setLocalDate(colName, scalaType))
  implicit val localTimeWriter: Writer[java.time.LocalTime] =
    make((bs, colName, scalaType) => bs.setLocalTime(colName, scalaType))
  implicit val longWriter: Writer[Long]           = make((bs, colName, scalaType) => bs.setLong(colName, scalaType))
  implicit val shortWriter: Writer[Short]         = make((bs, colName, scalaType) => bs.setShort(colName, scalaType))
  implicit val stringWriter: Writer[String]       = make((bs, colName, scalaType) => bs.setString(colName, scalaType))
  implicit val udtValueWriter: Writer[UdtValue]   = make((bs, colName, scalaType) => bs.setUdtValue(colName, scalaType))
  implicit val uuidWriter: Writer[java.util.UUID] = make((bs, colName, scalaType) => bs.setUuid(colName, scalaType))

  implicit def optionWriter[A](implicit ev: Writer[A]): Writer[Option[A]] =
    make((bs, colName, scalaType) =>
      scalaType match {
        case Some(value) => ev.write(bs, colName, value)
        case None        => bs.setToNull(colName)
      }
    )

  /**
   * Provides a lower level API so the user can have full control of how they
   * want to map their Scala type into a UDT Value
   */
  def writeUdtValue[ScalaType](f: (ScalaType, UserDefinedType) => UdtValue): Writer[ScalaType] = {
    (boundStatement: BoundStatementBuilder, column: String, value: ScalaType) =>
      val userDefinedType =
        boundStatement.getPreparedStatement.getVariableDefinitions
          .get(column)
          .getType
          .asInstanceOf[UserDefinedType]
      val udtValue = f(value, userDefinedType)
      boundStatement.setUdtValue(column, udtValue)
  }

  /**
   * Provides a lower level API so you can have full control of the low level
   * Datastax API. You would use this if you do not want to make use of
   * automatic derivation.
   * @param f
   * @tparam ScalaType
   * @return
   */
  def fromBoundStatementBuilder[ScalaType](
    f: (ScalaType, BoundStatementBuilder) => BoundStatementBuilder
  ): Writer[ScalaType] =
    (builder: BoundStatementBuilder, _: String, value: ScalaType) => f(value, builder)
}

trait TypeMapperSupport {
  implicit def deriveBinderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Writer[A] = {
    (statement: BoundStatementBuilder, columnName: String, value: A) =>
      val datatype        = statement.getType(columnName)
      val typeInformation = ev.classType
      val cassandra       = ev.toCassandra(value, datatype)
      statement.set(columnName, cassandra, typeInformation)
  }
}

trait MagnoliaWriterSupport {
  type Typeclass[T] = Writer[T]

  // Only supports case classes since this maps 1:1 with Cassandra's concept of a Row
  def join[T](ctx: CaseClass[Writer, T]): Writer[T] = new Writer[T] {
    override def write(builder: BoundStatementBuilder, column: String, value: T): BoundStatementBuilder =
      ctx.parameters.foldLeft(builder) { case (acc, param) =>
        val columnName = param.label
        val data       = param.dereference(value)
        param.typeclass.write(acc, columnName, data)
      }
  }

  // Semi automatic derivation to avoid conflict with CassandraTypeMapper
  def derive[T]: Writer[T] = macro Magnolia.gen[T]
}
