package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.data.{CqlDuration, SettableByName, TupleValue, UdtValue}
import io.kaizensolutions.virgil.annotations
import magnolia1._

import java.net.InetAddress
import java.nio.ByteBuffer
import java.time.{Instant, LocalDate, LocalTime}
import java.util.UUID
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

/**
 * Column Encoder for Cassandra data types.
 * @see
 *   https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/#cql-to-java-type-mapping
 * @tparam ScalaType
 */
@implicitNotFound(
  "No CqlColumnEncoder found for ${ScalaType}, please use UdtEncoder.derive for a User Defined Type or make use of <existingEncoderType>.contramap if you have a new type"
)
trait CqlColumnEncoder[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertScalaToDriver(scalaValue: ScalaType, dataType: DataType): DriverType

  def encodeFieldByName[Structure <: SettableByName[Structure]](
    fieldName: String,
    value: ScalaType,
    structure: Structure
  ): Structure

  def encodeFieldByIndex[Structure <: SettableByName[Structure]](
    index: Int,
    value: ScalaType,
    structure: Structure
  ): Structure

  def contramap[ScalaType2](f: ScalaType2 => ScalaType): CqlColumnEncoder[ScalaType2] =
    new CqlColumnEncoder[ScalaType2] {
      type DriverType = self.DriverType

      def driverClass: Class[DriverType] = self.driverClass

      def convertScalaToDriver(scalaValue: ScalaType2, dataType: DataType): DriverType =
        self.convertScalaToDriver(f(scalaValue), dataType)

      def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: ScalaType2,
        structure: Structure
      ): Structure =
        self.encodeFieldByName(fieldName, f(value), structure)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: ScalaType2,
        structure: Structure
      ): Structure =
        self.encodeFieldByIndex(index, f(value), structure)
    }
}
object CqlColumnEncoder extends UdtEncoderMagnoliaDerivation {
  type WithDriver[Sc, Dr] = CqlColumnEncoder[Sc] { type DriverType = Dr }

  def apply[ScalaType](implicit encoder: CqlColumnEncoder[ScalaType]): CqlColumnEncoder[ScalaType] = encoder

  def udt[A](f: (A, UdtValue) => UdtValue): CqlColumnEncoder.WithDriver[A, UdtValue] =
    new CqlColumnEncoder[A] {
      type DriverType = UdtValue

      def driverClass: Class[DriverType] = classOf[UdtValue]

      def convertScalaToDriver(scalaValue: A, dataType: DataType): DriverType =
        f(scalaValue, dataType.asInstanceOf[UserDefinedType].newValue())

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: A,
        structure: Structure
      ): Structure = {
        val dataType    = structure.getType(fieldName)
        val driverValue = convertScalaToDriver(value, dataType)
        structure.setUdtValue(fieldName, driverValue)
      }

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: A,
        structure: Structure
      ): Structure = {
        val dataType    = structure.getType(index)
        val driverValue = convertScalaToDriver(value, dataType)
        structure.setUdtValue(index, driverValue)
      }
    }

  // Primitives
  implicit val encoderForString: CqlColumnEncoder.WithDriver[String, java.lang.String] =
    new CqlColumnEncoder[String] {
      override type DriverType = java.lang.String

      override def driverClass: Class[DriverType] = classOf[java.lang.String]

      override def convertScalaToDriver(scalaValue: String, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: String,
        structure: Structure
      ): Structure =
        structure.setString(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: String,
        structure: Structure
      ): Structure =
        structure.setString(index, value)
    }

  implicit val encoderForInt: CqlColumnEncoder.WithDriver[Int, java.lang.Integer] =
    new CqlColumnEncoder[Int] {
      override type DriverType = java.lang.Integer

      override def driverClass: Class[DriverType] = classOf[java.lang.Integer]

      override def convertScalaToDriver(scalaValue: Int, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Int,
        structure: Structure
      ): Structure =
        structure.setInt(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Int,
        structure: Structure
      ): Structure =
        structure.setInt(index, value)
    }

  implicit val encoderForLong: CqlColumnEncoder.WithDriver[Long, java.lang.Long] =
    new CqlColumnEncoder[Long] {
      override type DriverType = java.lang.Long

      override def driverClass: Class[DriverType] = classOf[java.lang.Long]

      override def convertScalaToDriver(scalaValue: Long, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Long,
        structure: Structure
      ): Structure =
        structure.setLong(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Long,
        structure: Structure
      ): Structure =
        structure.setLong(index, value)
    }

  implicit val encoderForLocalDate: CqlColumnEncoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    new CqlColumnEncoder[java.time.LocalDate] {
      override type DriverType = java.time.LocalDate

      override def driverClass: Class[DriverType] = classOf[java.time.LocalDate]

      override def convertScalaToDriver(scalaValue: java.time.LocalDate, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.LocalDate,
        structure: Structure
      ): Structure =
        structure.setLocalDate(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: LocalDate,
        structure: Structure
      ): Structure =
        structure.setLocalDate(index, value)
    }

  implicit val encoderForLocalTime: CqlColumnEncoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    new CqlColumnEncoder[java.time.LocalTime] {
      override type DriverType = java.time.LocalTime

      override def driverClass: Class[DriverType] = classOf[java.time.LocalTime]

      override def convertScalaToDriver(scalaValue: java.time.LocalTime, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.LocalTime,
        structure: Structure
      ): Structure =
        structure.setLocalTime(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: LocalTime,
        structure: Structure
      ): Structure = structure.setLocalTime(index, value)
    }

  implicit val encoderForInstant: CqlColumnEncoder.WithDriver[java.time.Instant, java.time.Instant] =
    new CqlColumnEncoder[java.time.Instant] {
      override type DriverType = java.time.Instant

      override def driverClass: Class[DriverType] = classOf[java.time.Instant]

      override def convertScalaToDriver(scalaValue: java.time.Instant, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.time.Instant,
        structure: Structure
      ): Structure =
        structure.setInstant(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Instant,
        structure: Structure
      ): Structure = structure.setInstant(index, value)
    }

  implicit val encoderForByteBuffer: CqlColumnEncoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    new CqlColumnEncoder[java.nio.ByteBuffer] {
      override type DriverType = java.nio.ByteBuffer

      override def driverClass: Class[DriverType] = classOf[java.nio.ByteBuffer]

      override def convertScalaToDriver(scalaValue: java.nio.ByteBuffer, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.nio.ByteBuffer,
        structure: Structure
      ): Structure =
        structure.setByteBuffer(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: ByteBuffer,
        structure: Structure
      ): Structure = structure.setByteBuffer(index, value)
    }

  implicit val encoderForBoolean: CqlColumnEncoder.WithDriver[Boolean, java.lang.Boolean] =
    new CqlColumnEncoder[Boolean] {
      override type DriverType = java.lang.Boolean

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Boolean, dataType: DataType): DriverType = Boolean.box(scalaValue)

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Boolean,
        structure: Structure
      ): Structure = structure.setBoolean(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Boolean,
        structure: Structure
      ): Structure = structure.setBoolean(index, value)
    }

  implicit val encoderForBigDecimal: CqlColumnEncoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    new CqlColumnEncoder[BigDecimal] {
      override type DriverType = java.math.BigDecimal

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigDecimal, dataType: DataType): DriverType = scalaValue.bigDecimal

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: BigDecimal,
        structure: Structure
      ): Structure =
        structure.setBigDecimal(fieldName, value.bigDecimal)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: BigDecimal,
        structure: Structure
      ): Structure =
        structure.setBigDecimal(index, value.bigDecimal)
    }

  implicit val encoderForDouble: CqlColumnEncoder.WithDriver[Double, java.lang.Double] =
    new CqlColumnEncoder[Double] {
      override type DriverType = java.lang.Double

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Double, dataType: DataType): DriverType = Double.box(scalaValue)

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Double,
        structure: Structure
      ): Structure =
        structure.setDouble(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Double,
        structure: Structure
      ): Structure =
        structure.setDouble(index, value)
    }

  implicit val encoderForCqlDuration: CqlColumnEncoder.WithDriver[CqlDuration, CqlDuration] =
    new CqlColumnEncoder[CqlDuration] {
      override type DriverType = CqlDuration

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: CqlDuration, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: CqlDuration,
        structure: Structure
      ): Structure =
        structure.setCqlDuration(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: CqlDuration,
        structure: Structure
      ): Structure =
        structure.setCqlDuration(index, value)
    }

  implicit val encoderForFloat: CqlColumnEncoder.WithDriver[Float, java.lang.Float] =
    new CqlColumnEncoder[Float] {
      override type DriverType = java.lang.Float

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Float, dataType: DataType): DriverType = Float.box(scalaValue)

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Float,
        structure: Structure
      ): Structure =
        structure.setFloat(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Float,
        structure: Structure
      ): Structure =
        structure.setFloat(index, value)
    }

  implicit val encoderForInetAddress: CqlColumnEncoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    new CqlColumnEncoder[java.net.InetAddress] {
      override type DriverType = java.net.InetAddress

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.net.InetAddress, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.net.InetAddress,
        structure: Structure
      ): Structure =
        structure.setInetAddress(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: InetAddress,
        structure: Structure
      ): Structure =
        structure.setInetAddress(index, value)
    }

  implicit val encoderForShort: CqlColumnEncoder.WithDriver[Short, java.lang.Short] =
    new CqlColumnEncoder[Short] {
      override type DriverType = java.lang.Short

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Short, dataType: DataType): DriverType = Short.box(scalaValue)

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Short,
        structure: Structure
      ): Structure =
        structure.setShort(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Short,
        structure: Structure
      ): Structure = structure.setShort(index, value)
    }

  implicit val encoderForUUID: CqlColumnEncoder.WithDriver[java.util.UUID, java.util.UUID] =
    new CqlColumnEncoder[java.util.UUID] {
      override type DriverType = java.util.UUID

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: java.util.UUID, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: java.util.UUID,
        structure: Structure
      ): Structure =
        structure.setUuid(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: UUID,
        structure: Structure
      ): Structure =
        structure.setUuid(index, value)
    }

  implicit val encoderForByte: CqlColumnEncoder.WithDriver[Byte, java.lang.Byte] =
    new CqlColumnEncoder[Byte] {
      override type DriverType = java.lang.Byte

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Byte, dataType: DataType): DriverType = Byte.box(scalaValue)

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Byte,
        structure: Structure
      ): Structure =
        structure.setByte(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Byte,
        structure: Structure
      ): Structure =
        structure.setByte(index, value)
    }

  implicit val encoderForCqlTupleValue: CqlColumnEncoder.WithDriver[TupleValue, TupleValue] =
    new CqlColumnEncoder[TupleValue] {
      override type DriverType = TupleValue

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: TupleValue, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: TupleValue,
        structure: Structure
      ): Structure =
        structure.setTupleValue(fieldName, value)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: TupleValue,
        structure: Structure
      ): Structure =
        structure.setTupleValue(index, value)
    }

  implicit val encoderForBigInteger: CqlColumnEncoder.WithDriver[BigInt, java.math.BigInteger] =
    new CqlColumnEncoder[BigInt] {
      override type DriverType = java.math.BigInteger

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: BigInt, dataType: DataType): DriverType = scalaValue.bigInteger

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: BigInt,
        structure: Structure
      ): Structure =
        structure.setBigInteger(fieldName, value.bigInteger)

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: BigInt,
        structure: Structure
      ): Structure =
        structure.setBigInteger(index, value.bigInteger)
    }

  // Collections
  implicit def encoderForList[Elem](implicit ew: CqlColumnEncoder[Elem]): CqlColumnEncoder[List[Elem]] =
    new CqlColumnEncoder[List[Elem]] {
      self =>
      override type DriverType = java.util.List[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: List[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[ListType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: List[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(fieldName)
        structure.setList(fieldName, convertScalaToDriver(value, dataType), ew.driverClass)
      }

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: List[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(index)
        structure.setList(index, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def encoderForSet[Elem](implicit ew: CqlColumnEncoder[Elem]): CqlColumnEncoder[Set[Elem]] =
    new CqlColumnEncoder[Set[Elem]] {
      self =>
      override type DriverType = java.util.Set[ew.DriverType]

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: Set[Elem], dataType: DataType): DriverType = {
        val elemType = dataType.asInstanceOf[SetType].getElementType
        scalaValue.map(ew.convertScalaToDriver(_, elemType)).asJava
      }

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: Set[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(fieldName)
        structure.setSet(fieldName, convertScalaToDriver(value, dataType), ew.driverClass)
      }

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Set[Elem],
        structure: Structure
      ): Structure = {
        val dataType = structure.getType(index)
        structure.setSet(index, convertScalaToDriver(value, dataType), ew.driverClass)
      }
    }

  implicit def encoderForMap[Key, Value](implicit
    kw: CqlColumnEncoder[Key],
    vw: CqlColumnEncoder[Value]
  ): CqlColumnEncoder[Map[Key, Value]] = new CqlColumnEncoder[Map[Key, Value]] {
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

    override def encodeFieldByName[Structure <: SettableByName[Structure]](
      fieldName: String,
      value: Map[Key, Value],
      structure: Structure
    ): Structure = {
      val dataType = structure.getType(fieldName)
      structure.setMap(fieldName, convertScalaToDriver(value, dataType), kw.driverClass, vw.driverClass)
    }

    override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
      index: Int,
      value: Map[Key, Value],
      structure: Structure
    ): Structure = {
      val dataType = structure.getType(index)
      structure.setMap(index, convertScalaToDriver(value, dataType), kw.driverClass, vw.driverClass)
    }
  }

  implicit def encoderForOption[A](implicit ew: CqlColumnEncoder[A]): CqlColumnEncoder[Option[A]] =
    new CqlColumnEncoder[Option[A]] {
      self =>
      override type DriverType = ew.DriverType

      override def driverClass: Class[DriverType] = ew.driverClass

      override def convertScalaToDriver(scalaValue: Option[A], dataType: DataType): DriverType =
        scalaValue.map(ew.convertScalaToDriver(_, dataType)) match {
          case Some(value) => value
          case None        => null.asInstanceOf[DriverType]
        }

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
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

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: Option[A],
        structure: Structure
      ): Structure =
        value match {
          case Some(value) =>
            val dataType = structure.getType(index)
            structure.set(index, ew.convertScalaToDriver(value, dataType), ew.driverClass)

          case None =>
            structure.setToNull(index)
        }
    }
}

trait UdtEncoderMagnoliaDerivation {
  type Typeclass[T] = CqlColumnEncoder[T]

  // User Defined Types
  def join[T](ctx: CaseClass[CqlColumnEncoder, T]): CqlColumnEncoder.WithDriver[T, UdtValue] =
    CqlColumnEncoder.udt[T] { (scalaValue, udtValue) =>
      ctx.parameters.foldLeft(udtValue) { case (acc, p) =>
        val fieldName = {
          val default = p.label
          annotations.CqlColumn.extractFieldName(p.annotations).getOrElse(default)
        }
        p.typeclass.encodeFieldByName(fieldName, p.dereference(scalaValue), acc)
      }
    }

  implicit def deriveUdtValue[T]: CqlColumnEncoder.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
