package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.data.{CqlDuration, SettableByName, TupleValue, UdtValue}
import magnolia1._

import scala.jdk.CollectionConverters._

/**
 * Writer for Cassandra data types.
 * @see
 *   https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/#cql-to-java-type-mapping
 * @tparam ScalaType
 */
trait ColumnEncoder[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertScalaToDriver(scalaValue: ScalaType, dataType: DataType): DriverType

  def encodeField[Structure <: SettableByName[Structure]](
    fieldName: String,
    value: ScalaType,
    structure: Structure
  ): Structure

  def contramap[ScalaType2](f: ScalaType2 => ScalaType): ColumnEncoder[ScalaType2] =
    new ColumnEncoder[ScalaType2] {
      type DriverType = self.DriverType

      def driverClass: Class[DriverType] = self.driverClass

      def convertScalaToDriver(scalaValue: ScalaType2, dataType: DataType): DriverType =
        self.convertScalaToDriver(f(scalaValue), dataType)

      def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: ScalaType2,
        structure: Structure
      ): Structure =
        self.encodeField(fieldName, f(value), structure)
    }
}
object ColumnEncoder extends UdtWriterMagnoliaDerivation {
  type WithDriver[Sc, Dr] = ColumnEncoder[Sc] { type DriverType = Dr }

  def apply[ScalaType](implicit writer: ColumnEncoder[ScalaType]): ColumnEncoder[ScalaType] = writer

  def udt[A](f: (A, UdtValue) => UdtValue): ColumnEncoder.WithDriver[A, UdtValue] =
    new ColumnEncoder[A] {
      type DriverType = UdtValue

      def driverClass: Class[DriverType] = classOf[UdtValue]

      def convertScalaToDriver(scalaValue: A, dataType: DataType): DriverType =
        f(scalaValue, dataType.asInstanceOf[UserDefinedType].newValue())

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: A,
        structure: Structure
      ): Structure = {
        val dataType    = structure.getType(fieldName)
        val driverValue = convertScalaToDriver(value, dataType)
        structure.setUdtValue(fieldName, driverValue)
      }
    }

  // Primitives
  implicit val writerForString: ColumnEncoder.WithDriver[String, java.lang.String] =
    new ColumnEncoder[String] {
      override type DriverType = java.lang.String

      override def driverClass: Class[DriverType] = classOf[java.lang.String]

      override def convertScalaToDriver(scalaValue: String, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: String,
        structure: Structure
      ): Structure =
        structure.setString(fieldName, value)
    }

  implicit val writerForInt: ColumnEncoder.WithDriver[Int, java.lang.Integer] =
    new ColumnEncoder[Int] {
      override type DriverType = java.lang.Integer

      override def driverClass: Class[DriverType] = classOf[java.lang.Integer]

      override def convertScalaToDriver(scalaValue: Int, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Int,
        structure: Structure
      ): Structure =
        structure.setInt(fieldName, value)
    }

  implicit val writerForLong: ColumnEncoder.WithDriver[Long, java.lang.Long] =
    new ColumnEncoder[Long] {
      override type DriverType = java.lang.Long

      override def driverClass: Class[DriverType] = classOf[java.lang.Long]

      override def convertScalaToDriver(scalaValue: Long, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Long,
        structure: Structure
      ): Structure =
        structure.setLong(fieldName, value)
    }

  implicit val writerForLocalDate: ColumnEncoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    new ColumnEncoder[java.time.LocalDate] {
      override type DriverType = java.time.LocalDate

      override def driverClass: Class[DriverType] = classOf[java.time.LocalDate]

      override def convertScalaToDriver(scalaValue: java.time.LocalDate, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.LocalDate,
        structure: Structure
      ): Structure =
        structure.setLocalDate(fieldName, value)
    }

  implicit val writerForLocalTime: ColumnEncoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    new ColumnEncoder[java.time.LocalTime] {
      override type DriverType = java.time.LocalTime

      override def driverClass: Class[DriverType] = classOf[java.time.LocalTime]

      override def convertScalaToDriver(scalaValue: java.time.LocalTime, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.LocalTime,
        structure: Structure
      ): Structure =
        structure.setLocalTime(fieldName, value)
    }

  implicit val writerForInstant: ColumnEncoder.WithDriver[java.time.Instant, java.time.Instant] =
    new ColumnEncoder[java.time.Instant] {
      override type DriverType = java.time.Instant

      override def driverClass: Class[DriverType] = classOf[java.time.Instant]

      override def convertScalaToDriver(scalaValue: java.time.Instant, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.Instant,
        structure: Structure
      ): Structure =
        structure.setInstant(fieldName, value)
    }

  implicit val writerForByteBuffer: ColumnEncoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    new ColumnEncoder[java.nio.ByteBuffer] {
      override type DriverType = java.nio.ByteBuffer

      override def driverClass: Class[DriverType] = classOf[java.nio.ByteBuffer]

      override def convertScalaToDriver(scalaValue: java.nio.ByteBuffer, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.nio.ByteBuffer,
        structure: Structure
      ): Structure =
        structure.setByteBuffer(fieldName, value)
    }

  implicit val writerForBoolean: ColumnEncoder.WithDriver[Boolean, java.lang.Boolean] =
    new ColumnEncoder[Boolean] {
      override type DriverType = java.lang.Boolean

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Boolean, dataType: DataType): DriverType = Boolean.box(scalaValue)

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Boolean,
        structure: Structure
      ): Structure =
        structure.setBoolean(fieldName, value)
    }

  implicit val writerForBigDecimal: ColumnEncoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    new ColumnEncoder[BigDecimal] {
      override type DriverType = java.math.BigDecimal

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigDecimal, dataType: DataType): DriverType = scalaValue.bigDecimal

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: BigDecimal,
        structure: Structure
      ): Structure =
        structure.setBigDecimal(fieldName, value.bigDecimal)
    }

  implicit val writerForDouble: ColumnEncoder.WithDriver[Double, java.lang.Double] =
    new ColumnEncoder[Double] {
      override type DriverType = java.lang.Double

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Double, dataType: DataType): DriverType = Double.box(scalaValue)

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Double,
        structure: Structure
      ): Structure =
        structure.setDouble(fieldName, value)
    }

  implicit val writerForCqlDuration: ColumnEncoder.WithDriver[CqlDuration, CqlDuration] =
    new ColumnEncoder[CqlDuration] {
      override type DriverType = CqlDuration

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: CqlDuration, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: CqlDuration,
        structure: Structure
      ): Structure =
        structure.setCqlDuration(fieldName, value)
    }

  implicit val writerForFloat: ColumnEncoder.WithDriver[Float, java.lang.Float] =
    new ColumnEncoder[Float] {
      override type DriverType = java.lang.Float

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Float, dataType: DataType): DriverType = Float.box(scalaValue)

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Float,
        structure: Structure
      ): Structure =
        structure.setFloat(fieldName, value)
    }

  implicit val writerForInetAddress: ColumnEncoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    new ColumnEncoder[java.net.InetAddress] {
      override type DriverType = java.net.InetAddress

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.net.InetAddress, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.net.InetAddress,
        structure: Structure
      ): Structure =
        structure.setInetAddress(fieldName, value)
    }

  implicit val writerForShort: ColumnEncoder.WithDriver[Short, java.lang.Short] =
    new ColumnEncoder[Short] {
      override type DriverType = java.lang.Short

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Short, dataType: DataType): DriverType = Short.box(scalaValue)

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Short,
        structure: Structure
      ): Structure =
        structure.setShort(fieldName, value)
    }

  implicit val writerForUUID: ColumnEncoder.WithDriver[java.util.UUID, java.util.UUID] =
    new ColumnEncoder[java.util.UUID] {
      override type DriverType = java.util.UUID

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.util.UUID, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.util.UUID,
        structure: Structure
      ): Structure =
        structure.setUuid(fieldName, value)
    }

  implicit val writerForByte: ColumnEncoder.WithDriver[Byte, java.lang.Byte] =
    new ColumnEncoder[Byte] {
      override type DriverType = java.lang.Byte

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Byte, dataType: DataType): DriverType = Byte.box(scalaValue)

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Byte,
        structure: Structure
      ): Structure =
        structure.setByte(fieldName, value)
    }

  implicit val writerForCqlTupleValue: ColumnEncoder.WithDriver[TupleValue, TupleValue] =
    new ColumnEncoder[TupleValue] {
      override type DriverType = TupleValue

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: TupleValue, dataType: DataType): DriverType = scalaValue

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: TupleValue,
        structure: Structure
      ): Structure =
        structure.setTupleValue(fieldName, value)
    }

  implicit val writerForBigInteger: ColumnEncoder.WithDriver[BigInt, java.math.BigInteger] =
    new ColumnEncoder[BigInt] {
      override type DriverType = java.math.BigInteger

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigInt, dataType: DataType): DriverType = scalaValue.bigInteger

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: BigInt,
        structure: Structure
      ): Structure =
        structure.setBigInteger(fieldName, value.bigInteger)
    }

  // Collections
  implicit def writerForList[Elem](implicit ew: ColumnEncoder[Elem]): ColumnEncoder[List[Elem]] =
    new ColumnEncoder[List[Elem]] {
      self =>
      override type DriverType = java.util.List[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: List[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[ListType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: List[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(fieldName)
        structure.setList(fieldName, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def writerForSet[Elem](implicit ew: ColumnEncoder[Elem]): ColumnEncoder[Set[Elem]] =
    new ColumnEncoder[Set[Elem]] {
      self =>
      override type DriverType = java.util.Set[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Set[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[SetType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Set[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(fieldName)
        structure.setSet(fieldName, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def writerForMap[Key, Value](implicit
    kw: ColumnEncoder[Key],
    vw: ColumnEncoder[Value]
  ): ColumnEncoder[Map[Key, Value]] = new ColumnEncoder[Map[Key, Value]] {
    self =>
    override type DriverType = java.util.Map[kw.DriverType, vw.DriverType]

    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def convertScalaToDriver(scalaValue: Map[Key, Value], dataType: DataType): DriverType = {
      val mapType   = dataType.asInstanceOf[MapType]
      val keyType   = mapType.getKeyType
      val valueType = mapType.getValueType
      scalaValue.map { case (k, v) =>
        (kw.convertScalaToDriver(k, keyType), vw.convertScalaToDriver(v, valueType))
      }.asJava
    }

    override def encodeField[Structure <: SettableByName[Structure]](
      fieldName: String,
      value: Map[Key, Value],
      structure: Structure
    ): Structure = {
      val dataType = structure.getType(fieldName)
      structure.setMap(fieldName, convertScalaToDriver(value, dataType), kw.driverClass, vw.driverClass)
    }
  }

  implicit def writerForOption[A](implicit ew: ColumnEncoder[A]): ColumnEncoder[Option[A]] =
    new ColumnEncoder[Option[A]] {
      self =>
      override type DriverType = ew.DriverType

      override def driverClass: Class[DriverType] = ew.driverClass

      override def convertScalaToDriver(scalaValue: Option[A], dataType: DataType): DriverType =
        scalaValue.map(ew.convertScalaToDriver(_, dataType)) match {
          case Some(value) => value
          case None        => null.asInstanceOf[DriverType]
        }

      override def encodeField[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Option[A],
        structure: Structure
      ): Structure =
        value match {
          case Some(value) =>
            val dataType = structure.getType(fieldName)
            structure.set(fieldName, ew.convertScalaToDriver(value, dataType), ew.driverClass)

          case None =>
            structure.setToNull(fieldName)
        }
    }
}

trait UdtWriterMagnoliaDerivation {
  type Typeclass[T] = ColumnEncoder[T]

  // User Defined Types
  def join[T](ctx: CaseClass[ColumnEncoder, T]): ColumnEncoder.WithDriver[T, UdtValue] =
    ColumnEncoder.udt[T] { (scalaValue, udtValue) =>
      ctx.parameters.foldLeft(udtValue) { case (acc, p) =>
        p.typeclass.encodeField(p.label, p.dereference(scalaValue), acc)
      }
    }

  implicit def deriveUdtValue[T]: ColumnEncoder.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
