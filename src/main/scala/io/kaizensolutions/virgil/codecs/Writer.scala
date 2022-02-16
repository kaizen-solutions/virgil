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
trait Writer[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertScalaToDriver(scalaValue: ScalaType, dataType: DataType): DriverType

  def write[Structure <: SettableByName[Structure]](
    key: String,
    value: ScalaType,
    structure: Structure
  ): Structure

  def contramap[ScalaType2](f: ScalaType2 => ScalaType): Writer[ScalaType2] =
    new Writer[ScalaType2] {
      type DriverType = self.DriverType

      def driverClass: Class[DriverType] = self.driverClass

      def convertScalaToDriver(scalaValue: ScalaType2, dataType: DataType): DriverType =
        self.convertScalaToDriver(f(scalaValue), dataType)

      def write[Structure <: SettableByName[Structure]](
        key: String,
        value: ScalaType2,
        structure: Structure
      ): Structure =
        self.write(key, f(value), structure)
    }
}
object Writer extends UdtWriterMagnoliaDerivation {
  type WithDriver[Sc, Dr] = Writer[Sc] { type DriverType = Dr }

  def apply[ScalaType](implicit writer: Writer[ScalaType]): Writer[ScalaType] = writer

  def udt[A](f: (A, UdtValue) => UdtValue): Writer.WithDriver[A, UdtValue] =
    new Writer[A] {
      type DriverType = UdtValue

      def driverClass: Class[DriverType] = classOf[UdtValue]

      def convertScalaToDriver(scalaValue: A, dataType: DataType): DriverType =
        f(scalaValue, dataType.asInstanceOf[UserDefinedType].newValue())

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: A,
        structure: Structure
      ): Structure = {
        val dataType    = structure.getType(key)
        val driverValue = convertScalaToDriver(value, dataType)
        structure.setUdtValue(key, driverValue)
      }
    }

  // Primitives
  implicit val writerForString: Writer.WithDriver[String, java.lang.String] =
    new Writer[String] {
      override type DriverType = java.lang.String

      override def driverClass: Class[DriverType] = classOf[java.lang.String]

      override def convertScalaToDriver(scalaValue: String, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: String,
        structure: Structure
      ): Structure =
        structure.setString(key, value)
    }

  implicit val writerForInt: Writer.WithDriver[Int, java.lang.Integer] =
    new Writer[Int] {
      override type DriverType = java.lang.Integer

      override def driverClass: Class[DriverType] = classOf[java.lang.Integer]

      override def convertScalaToDriver(scalaValue: Int, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Int,
        structure: Structure
      ): Structure =
        structure.setInt(key, value)
    }

  implicit val writerForLong: Writer.WithDriver[Long, java.lang.Long] =
    new Writer[Long] {
      override type DriverType = java.lang.Long

      override def driverClass: Class[DriverType] = classOf[java.lang.Long]

      override def convertScalaToDriver(scalaValue: Long, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Long,
        structure: Structure
      ): Structure =
        structure.setLong(key, value)
    }

  implicit val writerForLocalDate: Writer.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    new Writer[java.time.LocalDate] {
      override type DriverType = java.time.LocalDate

      override def driverClass: Class[DriverType] = classOf[java.time.LocalDate]

      override def convertScalaToDriver(scalaValue: java.time.LocalDate, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.time.LocalDate,
        structure: Structure
      ): Structure =
        structure.setLocalDate(key, value)
    }

  implicit val writerForLocalTime: Writer.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    new Writer[java.time.LocalTime] {
      override type DriverType = java.time.LocalTime

      override def driverClass: Class[DriverType] = classOf[java.time.LocalTime]

      override def convertScalaToDriver(scalaValue: java.time.LocalTime, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.time.LocalTime,
        structure: Structure
      ): Structure =
        structure.setLocalTime(key, value)
    }

  implicit val writerForInstant: Writer.WithDriver[java.time.Instant, java.time.Instant] =
    new Writer[java.time.Instant] {
      override type DriverType = java.time.Instant

      override def driverClass: Class[DriverType] = classOf[java.time.Instant]

      override def convertScalaToDriver(scalaValue: java.time.Instant, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.time.Instant,
        structure: Structure
      ): Structure =
        structure.setInstant(key, value)
    }

  implicit val writerForByteBuffer: Writer.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    new Writer[java.nio.ByteBuffer] {
      override type DriverType = java.nio.ByteBuffer

      override def driverClass: Class[DriverType] = classOf[java.nio.ByteBuffer]

      override def convertScalaToDriver(scalaValue: java.nio.ByteBuffer, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.nio.ByteBuffer,
        structure: Structure
      ): Structure =
        structure.setByteBuffer(key, value)
    }

  implicit val writerForBoolean: Writer.WithDriver[Boolean, java.lang.Boolean] =
    new Writer[Boolean] {
      override type DriverType = java.lang.Boolean

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Boolean, dataType: DataType): DriverType = Boolean.box(scalaValue)

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Boolean,
        structure: Structure
      ): Structure =
        structure.setBoolean(key, value)
    }

  implicit val writerForBigDecimal: Writer.WithDriver[BigDecimal, java.math.BigDecimal] =
    new Writer[BigDecimal] {
      override type DriverType = java.math.BigDecimal

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigDecimal, dataType: DataType): DriverType = scalaValue.bigDecimal

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: BigDecimal,
        structure: Structure
      ): Structure =
        structure.setBigDecimal(key, value.bigDecimal)
    }

  implicit val writerForDouble: Writer.WithDriver[Double, java.lang.Double] =
    new Writer[Double] {
      override type DriverType = java.lang.Double

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Double, dataType: DataType): DriverType = Double.box(scalaValue)

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Double,
        structure: Structure
      ): Structure =
        structure.setDouble(key, value)
    }

  implicit val writerForCqlDuration: Writer.WithDriver[CqlDuration, CqlDuration] =
    new Writer[CqlDuration] {
      override type DriverType = CqlDuration

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: CqlDuration, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: CqlDuration,
        structure: Structure
      ): Structure =
        structure.setCqlDuration(key, value)
    }

  implicit val writerForFloat: Writer.WithDriver[Float, java.lang.Float] =
    new Writer[Float] {
      override type DriverType = java.lang.Float

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Float, dataType: DataType): DriverType = Float.box(scalaValue)

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Float,
        structure: Structure
      ): Structure =
        structure.setFloat(key, value)
    }

  implicit val writerForInetAddress: Writer.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    new Writer[java.net.InetAddress] {
      override type DriverType = java.net.InetAddress

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.net.InetAddress, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.net.InetAddress,
        structure: Structure
      ): Structure =
        structure.setInetAddress(key, value)
    }

  implicit val writerForShort: Writer.WithDriver[Short, java.lang.Short] =
    new Writer[Short] {
      override type DriverType = java.lang.Short

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Short, dataType: DataType): DriverType = Short.box(scalaValue)

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Short,
        structure: Structure
      ): Structure =
        structure.setShort(key, value)
    }

  implicit val writerForUUID: Writer.WithDriver[java.util.UUID, java.util.UUID] =
    new Writer[java.util.UUID] {
      override type DriverType = java.util.UUID

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.util.UUID, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: java.util.UUID,
        structure: Structure
      ): Structure =
        structure.setUuid(key, value)
    }

  implicit val writerForByte: Writer.WithDriver[Byte, java.lang.Byte] =
    new Writer[Byte] {
      override type DriverType = java.lang.Byte

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Byte, dataType: DataType): DriverType = Byte.box(scalaValue)

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Byte,
        structure: Structure
      ): Structure =
        structure.setByte(key, value)
    }

  implicit val writerForCqlTupleValue: Writer.WithDriver[TupleValue, TupleValue] = new Writer[TupleValue] {
    override type DriverType = TupleValue

    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def convertScalaToDriver(scalaValue: TupleValue, dataType: DataType): DriverType = scalaValue

    override def write[Structure <: SettableByName[Structure]](
      key: String,
      value: TupleValue,
      structure: Structure
    ): Structure =
      structure.setTupleValue(key, value)
  }

  implicit val writerForBigInteger: Writer.WithDriver[BigInt, java.math.BigInteger] =
    new Writer[BigInt] {
      override type DriverType = java.math.BigInteger

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigInt, dataType: DataType): DriverType = scalaValue.bigInteger

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: BigInt,
        structure: Structure
      ): Structure =
        structure.setBigInteger(key, value.bigInteger)
    }

  // Collections
  implicit def writerForList[Elem](implicit ew: Writer[Elem]): Writer[List[Elem]] =
    new Writer[List[Elem]] {
      self =>
      override type DriverType = java.util.List[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: List[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[ListType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: List[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(key)
        structure.setList(key, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def writerForSet[Elem](implicit ew: Writer[Elem]): Writer[Set[Elem]] =
    new Writer[Set[Elem]] {
      self =>
      override type DriverType = java.util.Set[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Set[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[SetType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Set[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(key)
        structure.setSet(key, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def writerForMap[Key, Value](implicit
    kw: Writer[Key],
    vw: Writer[Value]
  ): Writer[Map[Key, Value]] = new Writer[Map[Key, Value]] {
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

    override def write[Structure <: SettableByName[Structure]](
      key: String,
      value: Map[Key, Value],
      structure: Structure
    ): Structure = {
      val dataType = structure.getType(key)
      structure.setMap(key, convertScalaToDriver(value, dataType), kw.driverClass, vw.driverClass)
    }
  }

  implicit def writerForOption[A](implicit ew: Writer[A]): Writer[Option[A]] =
    new Writer[Option[A]] {
      self =>
      override type DriverType = ew.DriverType

      override def driverClass: Class[DriverType] = ew.driverClass

      override def convertScalaToDriver(scalaValue: Option[A], dataType: DataType): DriverType =
        scalaValue.map(ew.convertScalaToDriver(_, dataType)) match {
          case Some(value) => value
          case None        => null.asInstanceOf[DriverType]
        }

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: Option[A],
        structure: Structure
      ): Structure =
        value match {
          case Some(value) =>
            val dataType = structure.getType(key)
            structure.set(key, ew.convertScalaToDriver(value, dataType), ew.driverClass)

          case None =>
            structure.setToNull(key)
        }
    }
}

trait UdtWriterMagnoliaDerivation {
  type Typeclass[T] = Writer[T]

  // User Defined Types
  def join[T](ctx: CaseClass[Writer, T]): Writer.WithDriver[T, UdtValue] = new Writer[T] {
    override type DriverType = UdtValue

    override def driverClass: Class[DriverType] = classOf[UdtValue]

    override def write[Structure <: SettableByName[Structure]](
      key: String,
      value: T,
      structure: Structure
    ): Structure = {
      val dataType     = structure.getType(key)
      val subStructure = convertScalaToDriver(value, dataType)
      structure.setUdtValue(key, subStructure)
    }

    override def convertScalaToDriver(scalaValue: T, dataType: DataType): UdtValue = {
      val subStructure = dataType.asInstanceOf[UserDefinedType].newValue()
      ctx.parameters.foldLeft(subStructure) { case (s, p) =>
        p.typeclass.write(p.label, p.dereference(scalaValue), s)
      }
    }
  }

  implicit def derive[T]: Writer.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
