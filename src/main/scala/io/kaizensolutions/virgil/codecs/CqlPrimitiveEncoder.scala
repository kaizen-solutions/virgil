package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.data.{CqlDuration, SettableByName, UdtValue}
import zio.Chunk

import scala.jdk.CollectionConverters._

/**
 * A typeclass that describes how to turn a Scala type into a CQL type.
 *
 * @tparam ScalaType
 *   is the Scala type to be converted into the CQL type
 */
trait CqlPrimitiveEncoder[-ScalaType] { self =>
  type DriverType
  def driverClass: Class[DriverType]
  def scala2Driver(scalaValue: ScalaType, dataType: DataType): DriverType

  def contramap[ScalaType2](
    f: ScalaType2 => ScalaType
  ): CqlPrimitiveEncoder.WithDriver[ScalaType2, DriverType] =
    CqlPrimitiveEncoder.ContramapPrimitiveEncoder(self, f)
}
object CqlPrimitiveEncoder {
  type WithDriver[Scala, Driver] = CqlPrimitiveEncoder[Scala] { type DriverType = Driver }

  def apply[Scala](implicit encoder: CqlPrimitiveEncoder[Scala]): CqlPrimitiveEncoder[Scala] = encoder

  def encodePrimitiveByFieldName[Structure <: SettableByName[Structure], Scala](
    structure: Structure,
    fieldName: String,
    value: Scala
  )(implicit prim: CqlPrimitiveEncoder[Scala]): Structure =
    prim match {
      case StringPrimitiveEncoder      => structure.setString(fieldName, value)
      case BigIntPrimitiveEncoder      => structure.setBigInteger(fieldName, value.bigInteger)
      case ByteBufferPrimitiveEncoder  => structure.setByteBuffer(fieldName, value)
      case BooleanPrimitiveEncoder     => structure.setBoolean(fieldName, value)
      case LongPrimitiveEncoder        => structure.setLong(fieldName, value)
      case LocalDatePrimitiveEncoder   => structure.setLocalDate(fieldName, value)
      case BigDecimalPrimitiveEncoder  => structure.setBigDecimal(fieldName, value.bigDecimal)
      case DoublePrimitiveEncoder      => structure.setDouble(fieldName, value)
      case CqlDurationPrimitiveEncoder => structure.setCqlDuration(fieldName, value)
      case FloatPrimitiveEncoder       => structure.setFloat(fieldName, value)
      case InetAddressPrimitiveEncoder => structure.setInetAddress(fieldName, value)
      case IntPrimitiveEncoder         => structure.setInt(fieldName, value)
      case ShortPrimitiveEncoder       => structure.setShort(fieldName, value)
      case LocalTimePrimitiveEncoder   => structure.setLocalTime(fieldName, value)
      case InstantPrimitiveEncoder     => structure.setInstant(fieldName, value)
      case UUIDPrimitiveEncoder        => structure.setUuid(fieldName, value)
      case BytePrimitiveEncoder        => structure.setByte(fieldName, value)
      case UdtValuePrimitiveEncoder    => structure.setUdtValue(fieldName, value)

      case UdtValueEncoderPrimitiveEncoder(encoder) =>
        val emptyUdtValue = structure.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
        val udtValue      = encoder.encode(emptyUdtValue, value)
        structure.setUdtValue(fieldName, udtValue)

      case l @ ListPrimitiveEncoder(element, _) =>
        val driverType  = structure.getType(fieldName)
        val driverValue = l.scala2Driver(value, driverType)
        structure.setList(fieldName, driverValue, element.driverClass)

      case s @ SetPrimitiveEncoder(element) =>
        val driverType  = structure.getType(fieldName)
        val driverValue = s.scala2Driver(value, driverType)
        structure.setSet(fieldName, driverValue, element.driverClass)

      case m @ MapPrimitiveEncoder(k, v) =>
        val mapType   = structure.getType(fieldName)
        val driverMap = m.scala2Driver(value, mapType)
        structure.setMap(fieldName, driverMap, k.driverClass, v.driverClass)

      case o: OptionPrimitiveEncoder[scala, driver] =>
        value.asInstanceOf[Option[scala]] match {
          case Some(value) => encodePrimitiveByFieldName(structure, fieldName, value)(o.element)
          case None        => structure.setToNull(fieldName)
        }

      case ContramapPrimitiveEncoder(element, f) =>
        encodePrimitiveByFieldName(structure, fieldName, f(value))(element)

      case other =>
        val driverType  = structure.getType(fieldName)
        val driverValue = other.scala2Driver(value, driverType)
        structure.set(fieldName, driverValue, other.driverClass)
    }

  def encodePrimitiveByIndex[Structure <: SettableByName[Structure], Scala](
    structure: Structure,
    index: Int,
    value: Scala
  )(implicit prim: CqlPrimitiveEncoder[Scala]): Structure =
    prim match {
      case StringPrimitiveEncoder      => structure.setString(index, value)
      case BigIntPrimitiveEncoder      => structure.setBigInteger(index, value.bigInteger)
      case ByteBufferPrimitiveEncoder  => structure.setByteBuffer(index, value)
      case BooleanPrimitiveEncoder     => structure.setBoolean(index, value)
      case LongPrimitiveEncoder        => structure.setLong(index, value)
      case LocalDatePrimitiveEncoder   => structure.setLocalDate(index, value)
      case BigDecimalPrimitiveEncoder  => structure.setBigDecimal(index, value.bigDecimal)
      case DoublePrimitiveEncoder      => structure.setDouble(index, value)
      case CqlDurationPrimitiveEncoder => structure.setCqlDuration(index, value)
      case FloatPrimitiveEncoder       => structure.setFloat(index, value)
      case InetAddressPrimitiveEncoder => structure.setInetAddress(index, value)
      case IntPrimitiveEncoder         => structure.setInt(index, value)
      case ShortPrimitiveEncoder       => structure.setShort(index, value)
      case LocalTimePrimitiveEncoder   => structure.setLocalTime(index, value)
      case InstantPrimitiveEncoder     => structure.setInstant(index, value)
      case UUIDPrimitiveEncoder        => structure.setUuid(index, value)
      case BytePrimitiveEncoder        => structure.setByte(index, value)
      case UdtValuePrimitiveEncoder    => structure.setUdtValue(index, value)

      case UdtValueEncoderPrimitiveEncoder(encoder) =>
        val emptyUdtValue = structure.getType(index).asInstanceOf[UserDefinedType].newValue()
        val udtValue      = encoder.encode(emptyUdtValue, value)
        structure.setUdtValue(index, udtValue)

      case l @ ListPrimitiveEncoder(element, _) =>
        val driverType  = structure.getType(index).asInstanceOf[ListType].getElementType
        val driverValue = l.scala2Driver(value, driverType)
        structure.setList(index, driverValue, element.driverClass)

      case s @ SetPrimitiveEncoder(element) =>
        val driverType  = structure.getType(index).asInstanceOf[SetType]
        val driverValue = s.scala2Driver(value, driverType)
        structure.setSet(index, driverValue, element.driverClass)

      case m @ MapPrimitiveEncoder(k, v) =>
        val mapType   = structure.getType(index).asInstanceOf[MapType]
        val driverMap = m.scala2Driver(value, mapType)
        structure.setMap(index, driverMap, k.driverClass, v.driverClass)

      case o: OptionPrimitiveEncoder[scala, driver] =>
        value.asInstanceOf[Option[scala]] match {
          case Some(value) => encodePrimitiveByIndex(structure, index, value)(o.element)
          case None        => structure.setToNull(index)
        }

      case ContramapPrimitiveEncoder(element, f) =>
        encodePrimitiveByIndex(structure, index, f(value))(element)

      case other =>
        val driverType  = structure.getType(index)
        val driverValue = other.scala2Driver(value, driverType)
        structure.set(index, driverValue, other.driverClass)
    }

  case object StringPrimitiveEncoder extends CqlPrimitiveEncoder[String] {
    type DriverType = java.lang.String
    def driverClass: Class[DriverType]                                   = classOf[DriverType]
    def scala2Driver(scalaValue: String, dataType: DataType): DriverType = scalaValue
  }
  implicit val stringPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[String, java.lang.String] =
    StringPrimitiveEncoder

  case object BigIntPrimitiveEncoder extends CqlPrimitiveEncoder[BigInt] {
    type DriverType = java.math.BigInteger
    def driverClass: Class[DriverType]                                   = classOf[DriverType]
    def scala2Driver(scalaValue: BigInt, dataType: DataType): DriverType = scalaValue.bigInteger
  }
  implicit val bigIntPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[BigInt, java.math.BigInteger] =
    BigIntPrimitiveEncoder

  case object ByteBufferPrimitiveEncoder extends CqlPrimitiveEncoder[java.nio.ByteBuffer] {
    type DriverType = java.nio.ByteBuffer
    def driverClass: Class[DriverType]                                                = classOf[DriverType]
    def scala2Driver(scalaValue: java.nio.ByteBuffer, dataType: DataType): DriverType = scalaValue
  }
  implicit val byteBufferPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    ByteBufferPrimitiveEncoder

  case object BooleanPrimitiveEncoder extends CqlPrimitiveEncoder[Boolean] {
    type DriverType = java.lang.Boolean
    def driverClass: Class[DriverType]                                    = classOf[DriverType]
    def scala2Driver(scalaValue: Boolean, dataType: DataType): DriverType = scalaValue
  }
  implicit val booleanPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Boolean, java.lang.Boolean] =
    BooleanPrimitiveEncoder

  case object LongPrimitiveEncoder extends CqlPrimitiveEncoder[Long] {
    type DriverType = java.lang.Long
    def driverClass: Class[DriverType]                                 = classOf[DriverType]
    def scala2Driver(scalaValue: Long, dataType: DataType): DriverType = scalaValue
  }
  implicit val longPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Long, java.lang.Long] =
    LongPrimitiveEncoder

  case object LocalDatePrimitiveEncoder extends CqlPrimitiveEncoder[java.time.LocalDate] {
    type DriverType = java.time.LocalDate
    def driverClass: Class[DriverType]                                                = classOf[DriverType]
    def scala2Driver(scalaValue: java.time.LocalDate, dataType: DataType): DriverType = scalaValue
  }
  implicit val datePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    LocalDatePrimitiveEncoder

  case object BigDecimalPrimitiveEncoder extends CqlPrimitiveEncoder[BigDecimal] {
    type DriverType = java.math.BigDecimal
    def driverClass: Class[DriverType]                                       = classOf[DriverType]
    def scala2Driver(scalaValue: BigDecimal, dataType: DataType): DriverType = scalaValue.bigDecimal
  }
  implicit val bigDecimalPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    BigDecimalPrimitiveEncoder

  case object DoublePrimitiveEncoder extends CqlPrimitiveEncoder[Double] {
    type DriverType = java.lang.Double
    def driverClass: Class[DriverType]                                   = classOf[DriverType]
    def scala2Driver(scalaValue: Double, dataType: DataType): DriverType = scalaValue
  }
  implicit val doublePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Double, java.lang.Double] =
    DoublePrimitiveEncoder

  case object CqlDurationPrimitiveEncoder extends CqlPrimitiveEncoder[CqlDuration] {
    type DriverType = CqlDuration
    def driverClass: Class[DriverType]                                        = classOf[DriverType]
    def scala2Driver(scalaValue: CqlDuration, dataType: DataType): DriverType = scalaValue
  }
  implicit val cqlDurationPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[CqlDuration, CqlDuration] =
    CqlDurationPrimitiveEncoder

  case object FloatPrimitiveEncoder extends CqlPrimitiveEncoder[Float] {
    type DriverType = java.lang.Float
    def driverClass: Class[DriverType]                                  = classOf[DriverType]
    def scala2Driver(scalaValue: Float, dataType: DataType): DriverType = scalaValue
  }
  implicit val floatPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Float, java.lang.Float] =
    FloatPrimitiveEncoder

  case object InetAddressPrimitiveEncoder extends CqlPrimitiveEncoder[java.net.InetAddress] {
    type DriverType = java.net.InetAddress
    def driverClass: Class[DriverType]                                                 = classOf[DriverType]
    def scala2Driver(scalaValue: java.net.InetAddress, dataType: DataType): DriverType = scalaValue
  }
  implicit val inetPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    InetAddressPrimitiveEncoder

  case object IntPrimitiveEncoder extends CqlPrimitiveEncoder[Int] {
    type DriverType = java.lang.Integer
    def driverClass: Class[DriverType]                                = classOf[DriverType]
    def scala2Driver(scalaValue: Int, dataType: DataType): DriverType = scalaValue
  }
  implicit val intPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Int, java.lang.Integer] =
    IntPrimitiveEncoder

  case object ShortPrimitiveEncoder extends CqlPrimitiveEncoder[Short] {
    type DriverType = java.lang.Short
    def driverClass: Class[DriverType]                                  = classOf[DriverType]
    def scala2Driver(scalaValue: Short, dataType: DataType): DriverType = scalaValue
  }
  implicit val shortPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Short, java.lang.Short] =
    ShortPrimitiveEncoder

  case object LocalTimePrimitiveEncoder extends CqlPrimitiveEncoder[java.time.LocalTime] {
    type DriverType = java.time.LocalTime
    def driverClass: Class[DriverType]                                                = classOf[DriverType]
    def scala2Driver(scalaValue: java.time.LocalTime, dataType: DataType): DriverType = scalaValue
  }
  implicit val localTimePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    LocalTimePrimitiveEncoder

  case object InstantPrimitiveEncoder extends CqlPrimitiveEncoder[java.time.Instant] {
    type DriverType = java.time.Instant
    def driverClass: Class[DriverType]                                              = classOf[DriverType]
    def scala2Driver(scalaValue: java.time.Instant, dataType: DataType): DriverType = scalaValue
  }
  implicit val instantPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.Instant, java.time.Instant] =
    InstantPrimitiveEncoder

  case object UUIDPrimitiveEncoder extends CqlPrimitiveEncoder[java.util.UUID] {
    type DriverType = java.util.UUID
    def driverClass: Class[DriverType]                                           = classOf[DriverType]
    def scala2Driver(scalaValue: java.util.UUID, dataType: DataType): DriverType = scalaValue
  }
  implicit val uuidPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.util.UUID, java.util.UUID] =
    UUIDPrimitiveEncoder

  case object BytePrimitiveEncoder extends CqlPrimitiveEncoder[Byte] {
    type DriverType = java.lang.Byte
    def driverClass: Class[DriverType]                                 = classOf[DriverType]
    def scala2Driver(scalaValue: Byte, dataType: DataType): DriverType = scalaValue
  }
  implicit val bytePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Byte, java.lang.Byte] =
    BytePrimitiveEncoder

  case object UdtValuePrimitiveEncoder extends CqlPrimitiveEncoder[UdtValue] {
    type DriverType = UdtValue
    def driverClass: Class[DriverType]                                     = classOf[DriverType]
    def scala2Driver(scalaValue: UdtValue, dataType: DataType): DriverType = scalaValue
  }
  implicit val udtValuePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[UdtValue, UdtValue] =
    UdtValuePrimitiveEncoder

  final case class UdtValueEncoderPrimitiveEncoder[A](encoder: CqlUdtValueEncoder.Object[A])
      extends CqlPrimitiveEncoder[A] {
    type DriverType = UdtValue
    def driverClass: Class[DriverType] = classOf[DriverType]
    def scala2Driver(scalaValue: A, dataType: DataType): DriverType = {
      val udtValue = dataType.asInstanceOf[UserDefinedType].newValue()
      encoder.encode(udtValue, scalaValue)
    }
  }
  implicit def scalaTypeViaUdtValuePrimitive[A](implicit
    encoder: CqlUdtValueEncoder.Object[A]
  ): CqlPrimitiveEncoder[A] =
    UdtValueEncoderPrimitiveEncoder(encoder)

  final case class ListPrimitiveEncoder[Collection[_], ScalaElem, DriverElem](
    element: CqlPrimitiveEncoder.WithDriver[ScalaElem, DriverElem],
    transform: (Collection[ScalaElem], Function[ScalaElem, DriverElem]) => java.util.List[DriverElem]
  ) extends CqlPrimitiveEncoder[Collection[ScalaElem]] {
    override type DriverType = java.util.List[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def scala2Driver(scalaCollection: Collection[ScalaElem], dataType: DataType): DriverType = {
      val elementDataType = dataType.asInstanceOf[ListType].getElementType
      transform(scalaCollection, element.scala2Driver(_, elementDataType))
    }
  }

  implicit def listCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[List[A], java.util.List[element.DriverType]] =
    ListPrimitiveEncoder[List, A, element.DriverType](element, (list, transform) => list.map(transform).asJava)

  implicit def chunkCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[Chunk[A], java.util.List[element.DriverType]] =
    ListPrimitiveEncoder[Chunk, A, element.DriverType](element, (chunk, transform) => chunk.map(transform).asJava)

  implicit def vectorCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[Vector[A], java.util.List[element.DriverType]] =
    ListPrimitiveEncoder[Vector, A, element.DriverType](element, (vector, transform) => vector.map(transform).asJava)

  final case class SetPrimitiveEncoder[Scala, Driver](element: CqlPrimitiveEncoder.WithDriver[Scala, Driver])
      extends CqlPrimitiveEncoder[Set[Scala]] {
    override type DriverType = java.util.Set[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def scala2Driver(scalaValue: Set[Scala], dataType: DataType): DriverType = {
      val elementDataType = dataType.asInstanceOf[SetType].getElementType
      scalaValue.map(element.scala2Driver(_, elementDataType)).asJava
    }
  }
  implicit def setCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[Set[A], java.util.Set[element.DriverType]] =
    SetPrimitiveEncoder[A, element.DriverType](element)

  final case class MapPrimitiveEncoder[K, DriverK, V, DriverV](
    key: CqlPrimitiveEncoder.WithDriver[K, DriverK],
    value: CqlPrimitiveEncoder.WithDriver[V, DriverV]
  ) extends CqlPrimitiveEncoder[Map[K, V]] {
    override type DriverType = java.util.Map[DriverK, DriverV]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def scala2Driver(scalaValue: Map[K, V], dataType: DataType): DriverType = {
      val mapType       = dataType.asInstanceOf[MapType]
      val keyDataType   = mapType.getKeyType
      val valueDataType = mapType.getValueType
      scalaValue.map { case (k, v) =>
        key.scala2Driver(k, keyDataType) -> value.scala2Driver(v, valueDataType)
      }.asJava
    }
  }

  implicit def mapCqlPrimitiveEncoder[K, V](implicit
    key: CqlPrimitiveEncoder[K],
    value: CqlPrimitiveEncoder[V]
  ): CqlPrimitiveEncoder.WithDriver[Map[K, V], java.util.Map[key.DriverType, value.DriverType]] =
    MapPrimitiveEncoder[K, key.DriverType, V, value.DriverType](key, value)

  final case class OptionPrimitiveEncoder[Scala, Driver](element: CqlPrimitiveEncoder.WithDriver[Scala, Driver])
      extends CqlPrimitiveEncoder[Option[Scala]] {
    override type DriverType = element.DriverType
    override def driverClass: Class[DriverType] = element.driverClass
    override def scala2Driver(scalaValue: Option[Scala], dataType: DataType): DriverType =
      scalaValue match {
        case Some(value) => element.scala2Driver(value, dataType)
        case None        => null.asInstanceOf[DriverType]
      }
  }
  implicit def optionCqlPrimitiveEncoder[A](implicit element: CqlPrimitiveEncoder[A]): CqlPrimitiveEncoder[Option[A]] =
    OptionPrimitiveEncoder[A, element.DriverType](element)

  final case class ContramapPrimitiveEncoder[Scala, Scala2, Driver](
    element: CqlPrimitiveEncoder.WithDriver[Scala, Driver],
    f: Scala2 => Scala
  ) extends CqlPrimitiveEncoder[Scala2] {
    override type DriverType = Driver
    override def driverClass: Class[DriverType] = element.driverClass
    override def scala2Driver(scalaValue: Scala2, dataType: DataType): DriverType =
      element.scala2Driver(f(scalaValue), dataType)
  }
}
