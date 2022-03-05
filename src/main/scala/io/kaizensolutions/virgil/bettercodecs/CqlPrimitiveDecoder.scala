package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.`type`.{DataType, ListType, MapType, SetType}
import com.datastax.oss.driver.api.core.data.{CqlDuration, GettableByIndex, GettableByName, UdtValue}

import scala.jdk.CollectionConverters._

/**
 * A typeclass that describes how to turn a Scala type into a CQL type.
 *
 * @tparam ScalaType
 *   is the Scala type to be converted into the CQL type
 */
trait CqlPrimitiveDecoder[ScalaType] { self =>
  type DriverType
  def driverClass: Class[DriverType]
  def driver2Scala(driverValue: DriverType, dataType: DataType): ScalaType

  def map[ScalaType2](f: ScalaType => ScalaType2): CqlPrimitiveDecoder[ScalaType2] =
    CqlPrimitiveDecoder.MapFunctionPrimitiveDecoder[ScalaType, ScalaType2, DriverType](self, f)
}

object CqlPrimitiveDecoder {
  type WithDriver[Scala, Driver] = CqlPrimitiveDecoder[Scala] { type DriverType = Driver }

  def apply[Scala](implicit decoder: CqlPrimitiveDecoder[Scala]): CqlPrimitiveDecoder[Scala] = decoder

  def decodePrimitiveByFieldName[Structure <: GettableByName, Scala](structure: Structure, fieldName: String)(implicit
    prim: CqlPrimitiveDecoder[Scala]
  ): Scala = prim match {
    // These special cases allow us to avoid extra calls to the registry and extra function calls
    case StringPrimitiveDecoder                   => structure.getString(fieldName)
    case BigIntPrimitiveDecoder                   => structure.getBigInteger(fieldName)
    case ByteBufferPrimitiveDecoder               => structure.getByteBuffer(fieldName)
    case BooleanPrimitiveDecoder                  => structure.getBoolean(fieldName)
    case LongPrimitiveDecoder                     => structure.getLong(fieldName)
    case LocalDatePrimitiveDecoder                => structure.getLocalDate(fieldName)
    case BigDecimalPrimitiveDecoder               => structure.getBigDecimal(fieldName)
    case DoublePrimitiveDecoder                   => structure.getDouble(fieldName)
    case CqlDurationPrimitiveDecoder              => structure.getCqlDuration(fieldName)
    case FloatPrimitiveDecoder                    => structure.getFloat(fieldName)
    case InetAddressPrimitiveDecoder              => structure.getInetAddress(fieldName)
    case IntPrimitiveDecoder                      => structure.getInt(fieldName)
    case ShortPrimitiveDecoder                    => structure.getShort(fieldName)
    case LocalTimePrimitiveDecoder                => structure.getLocalTime(fieldName)
    case InstantPrimitiveDecoder                  => structure.getInstant(fieldName)
    case UUIDPrimitiveDecoder                     => structure.getUuid(fieldName)
    case BytePrimitiveDecoder                     => structure.getByte(fieldName)
    case UdtValuePrimitiveDecoder                 => structure.getUdtValue(fieldName)
    case UdtValueDecoderPrimitiveDecoder(decoder) => decoder.decode(structure.getUdtValue(fieldName))

    // Collections
    case l @ ListPrimitiveDecoder(element) =>
      l.driver2Scala(
        structure.getList(fieldName, element.driverClass),
        structure.getType(fieldName)
      )

    case s @ SetPrimitiveDecoder(element) =>
      s.driver2Scala(
        structure.getSet(fieldName, element.driverClass),
        structure.getType(fieldName)
      )

    case m @ MapPrimitiveDecoder(key, value) =>
      m.driver2Scala(
        structure.getMap(fieldName, key.driverClass, value.driverClass),
        structure.getType(fieldName)
      )

    case MapFunctionPrimitiveDecoder(original, f) =>
      f(decodePrimitiveByFieldName(structure, fieldName)(original))

    // Rely on using get + classType which causes a registry lookup which is slower for all other cases
    case other => other.driver2Scala(structure.get(fieldName, other.driverClass), structure.getType(fieldName))
  }

  def decodePrimitiveByIndex[Structure <: GettableByIndex, Scala](structure: Structure, index: Int)(implicit
    prim: CqlPrimitiveDecoder[Scala]
  ): Scala = prim match {
    // These special cases allow us to avoid extra calls to the registry and extra function calls
    case StringPrimitiveDecoder                   => structure.getString(index)
    case BigIntPrimitiveDecoder                   => structure.getBigInteger(index)
    case ByteBufferPrimitiveDecoder               => structure.getByteBuffer(index)
    case BooleanPrimitiveDecoder                  => structure.getBoolean(index)
    case LongPrimitiveDecoder                     => structure.getLong(index)
    case LocalDatePrimitiveDecoder                => structure.getLocalDate(index)
    case BigDecimalPrimitiveDecoder               => structure.getBigDecimal(index)
    case DoublePrimitiveDecoder                   => structure.getDouble(index)
    case CqlDurationPrimitiveDecoder              => structure.getCqlDuration(index)
    case FloatPrimitiveDecoder                    => structure.getFloat(index)
    case InetAddressPrimitiveDecoder              => structure.getInetAddress(index)
    case IntPrimitiveDecoder                      => structure.getInt(index)
    case ShortPrimitiveDecoder                    => structure.getShort(index)
    case LocalTimePrimitiveDecoder                => structure.getLocalTime(index)
    case InstantPrimitiveDecoder                  => structure.getInstant(index)
    case UUIDPrimitiveDecoder                     => structure.getUuid(index)
    case BytePrimitiveDecoder                     => structure.getByte(index)
    case UdtValuePrimitiveDecoder                 => structure.getUdtValue(index)
    case UdtValueDecoderPrimitiveDecoder(decoder) => decoder.decode(structure.getUdtValue(index))

    // Collections
    case l @ ListPrimitiveDecoder(element) =>
      l.driver2Scala(
        structure.getList(index, element.driverClass),
        structure.getType(index)
      )

    case s @ SetPrimitiveDecoder(element) =>
      s.driver2Scala(
        structure.getSet(index, element.driverClass),
        structure.getType(index)
      )

    case m @ MapPrimitiveDecoder(key, value) =>
      m.driver2Scala(
        structure.getMap(index, key.driverClass, value.driverClass),
        structure.getType(index)
      )

    case MapFunctionPrimitiveDecoder(original, f) =>
      f(decodePrimitiveByIndex(structure, index)(original))

    // Rely on using get + classType which causes a registry lookup which is slower
    case other => other.driver2Scala(structure.get(index, other.driverClass), structure.getType(index))
  }

  case object StringPrimitiveDecoder extends CqlPrimitiveDecoder[String] {
    type DriverType = java.lang.String
    def driverClass: Class[DriverType]                                    = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): String = driverValue
  }
  implicit val stringPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[String, java.lang.String] =
    StringPrimitiveDecoder

  case object BigIntPrimitiveDecoder extends CqlPrimitiveDecoder[BigInt] {
    type DriverType = java.math.BigInteger
    def driverClass: Class[DriverType]                                    = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): BigInt = BigInt(driverValue)
  }
  implicit val bigIntPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[BigInt, java.math.BigInteger] =
    BigIntPrimitiveDecoder

  case object ByteBufferPrimitiveDecoder extends CqlPrimitiveDecoder[java.nio.ByteBuffer] {
    type DriverType = java.nio.ByteBuffer
    def driverClass: Class[DriverType]                                                 = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.nio.ByteBuffer = driverValue
  }
  implicit val byteBufferPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    ByteBufferPrimitiveDecoder

  case object BooleanPrimitiveDecoder extends CqlPrimitiveDecoder[Boolean] {
    type DriverType = java.lang.Boolean
    def driverClass: Class[DriverType]                                     = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Boolean = driverValue
  }
  implicit val booleanPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Boolean, java.lang.Boolean] =
    BooleanPrimitiveDecoder

  case object LongPrimitiveDecoder extends CqlPrimitiveDecoder[Long] {
    type DriverType = java.lang.Long
    def driverClass: Class[DriverType]                                  = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Long = driverValue
  }
  implicit val longPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Long, java.lang.Long] =
    LongPrimitiveDecoder

  case object LocalDatePrimitiveDecoder extends CqlPrimitiveDecoder[java.time.LocalDate] {
    type DriverType = java.time.LocalDate
    def driverClass: Class[DriverType]                                                 = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.LocalDate = driverValue
  }
  implicit val localDatePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    LocalDatePrimitiveDecoder

  case object BigDecimalPrimitiveDecoder extends CqlPrimitiveDecoder[BigDecimal] {
    type DriverType = java.math.BigDecimal
    def driverClass: Class[DriverType]                                        = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): BigDecimal = BigDecimal(driverValue)
  }
  implicit val bigDecimalPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    BigDecimalPrimitiveDecoder

  case object DoublePrimitiveDecoder extends CqlPrimitiveDecoder[Double] {
    type DriverType = java.lang.Double
    def driverClass: Class[DriverType]                                    = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Double = driverValue
  }
  implicit val doublePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Double, java.lang.Double] =
    DoublePrimitiveDecoder

  case object CqlDurationPrimitiveDecoder extends CqlPrimitiveDecoder[CqlDuration] {
    type DriverType = CqlDuration
    def driverClass: Class[DriverType]                                         = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): CqlDuration = driverValue
  }
  implicit val cqlDurationPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[CqlDuration, CqlDuration] =
    CqlDurationPrimitiveDecoder

  case object FloatPrimitiveDecoder extends CqlPrimitiveDecoder[Float] {
    type DriverType = java.lang.Float
    def driverClass: Class[DriverType]                                   = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Float = driverValue
  }
  implicit val floatPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Float, java.lang.Float] =
    FloatPrimitiveDecoder

  case object InetAddressPrimitiveDecoder extends CqlPrimitiveDecoder[java.net.InetAddress] {
    type DriverType = java.net.InetAddress
    def driverClass: Class[DriverType]                                                  = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.net.InetAddress = driverValue
  }
  implicit val inetAddressPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    InetAddressPrimitiveDecoder

  case object IntPrimitiveDecoder extends CqlPrimitiveDecoder[Int] {
    type DriverType = java.lang.Integer
    def driverClass: Class[DriverType]                                 = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Int = driverValue
  }
  implicit val intPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Int, java.lang.Integer] =
    IntPrimitiveDecoder

  case object ShortPrimitiveDecoder extends CqlPrimitiveDecoder[Short] {
    type DriverType = java.lang.Short
    def driverClass: Class[DriverType]                                   = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Short = driverValue
  }
  implicit val shortPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Short, java.lang.Short] =
    ShortPrimitiveDecoder

  case object LocalTimePrimitiveDecoder extends CqlPrimitiveDecoder[java.time.LocalTime] {
    type DriverType = java.time.LocalTime
    def driverClass: Class[DriverType]                                                 = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.LocalTime = driverValue
  }
  implicit val localTimePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    LocalTimePrimitiveDecoder

  case object InstantPrimitiveDecoder extends CqlPrimitiveDecoder[java.time.Instant] {
    type DriverType = java.time.Instant
    def driverClass: Class[DriverType]                                               = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.Instant = driverValue
  }
  implicit val instantPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.Instant, java.time.Instant] =
    InstantPrimitiveDecoder

  case object UUIDPrimitiveDecoder extends CqlPrimitiveDecoder[java.util.UUID] {
    type DriverType = java.util.UUID
    def driverClass: Class[DriverType]                                            = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): java.util.UUID = driverValue
  }
  implicit val uuidPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.util.UUID, java.util.UUID] =
    UUIDPrimitiveDecoder

  case object BytePrimitiveDecoder extends CqlPrimitiveDecoder[Byte] {
    type DriverType = java.lang.Byte
    def driverClass: Class[DriverType]                                  = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): Byte = driverValue
  }
  implicit val bytePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Byte, java.lang.Byte] =
    BytePrimitiveDecoder

  case object UdtValuePrimitiveDecoder extends CqlPrimitiveDecoder[UdtValue] {
    type DriverType = UdtValue
    def driverClass: Class[DriverType]                                      = classOf[DriverType]
    def driver2Scala(driverValue: DriverType, dataType: DataType): UdtValue = driverValue
  }
  implicit val udtValuePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[UdtValue, UdtValue] =
    UdtValuePrimitiveDecoder

  final case class UdtValueDecoderPrimitiveDecoder[A](decoder: UdtValueDecoder.Object[A])
      extends CqlPrimitiveDecoder[A] {
    type DriverType = UdtValue
    def driverClass: Class[DriverType] = classOf[DriverType]

    def driver2Scala(driverValue: DriverType, dataType: DataType): A =
      decoder.decode(driverValue)
  }
  implicit def scalaTypeViaUdtValuePrimitive[A](implicit
    decoder: UdtValueDecoder.Object[A]
  ): CqlPrimitiveDecoder.WithDriver[A, UdtValue] =
    UdtValueDecoderPrimitiveDecoder(decoder)

  final case class ListPrimitiveDecoder[Scala, Driver](element: CqlPrimitiveDecoder.WithDriver[Scala, Driver])
      extends CqlPrimitiveDecoder[List[Scala]] {
    override type DriverType = java.util.List[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def driver2Scala(driverValue: DriverType, dataType: DataType): List[Scala] = {
      val elementDataType = dataType.asInstanceOf[ListType].getElementType
      driverValue.asScala.map(element.driver2Scala(_, elementDataType)).toList
    }
  }
  implicit def listCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[List[A], java.util.List[element.DriverType]] =
    ListPrimitiveDecoder(element)

  final case class SetPrimitiveDecoder[Scala, Driver](element: CqlPrimitiveDecoder.WithDriver[Scala, Driver])
      extends CqlPrimitiveDecoder[Set[Scala]] {
    override type DriverType = java.util.Set[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def driver2Scala(driverValue: DriverType, dataType: DataType): Set[Scala] = {
      val elementDataType = dataType.asInstanceOf[SetType].getElementType
      driverValue.asScala.map(element.driver2Scala(_, elementDataType)).toSet
    }
  }
  implicit def setCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[Set[A], java.util.Set[element.DriverType]] =
    SetPrimitiveDecoder(element)

  final case class MapPrimitiveDecoder[K, DriverK, V, DriverV](
    key: CqlPrimitiveDecoder.WithDriver[K, DriverK],
    value: CqlPrimitiveDecoder.WithDriver[V, DriverV]
  ) extends CqlPrimitiveDecoder[Map[K, V]] {
    override type DriverType = java.util.Map[key.DriverType, value.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def driver2Scala(driverValue: DriverType, dataType: DataType): Map[K, V] = {
      val mapType       = dataType.asInstanceOf[MapType]
      val keyDataType   = mapType.getKeyType
      val valueDataType = mapType.getValueType
      driverValue.asScala.map { case (k, v) =>
        key.driver2Scala(k, keyDataType) -> value.driver2Scala(v, valueDataType)
      }.toMap
    }
  }
  implicit def mapCqlPrimitiveDecoder[A, B](implicit
    key: CqlPrimitiveDecoder[A],
    value: CqlPrimitiveDecoder[B]
  ): CqlPrimitiveDecoder.WithDriver[Map[A, B], java.util.Map[key.DriverType, value.DriverType]] =
    MapPrimitiveDecoder(key, value)

  final case class OptionPrimitiveDecoder[Scala, Driver](element: CqlPrimitiveDecoder.WithDriver[Scala, Driver])
      extends CqlPrimitiveDecoder[Option[Scala]] {
    override type DriverType = element.DriverType
    override def driverClass: Class[DriverType] = element.driverClass

    override def driver2Scala(driverValue: DriverType, dataType: DataType): Option[Scala] =
      Option(driverValue).map(element.driver2Scala(_, dataType))
  }
  implicit def optionCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[Option[A], element.DriverType] =
    OptionPrimitiveDecoder(element)

  final case class MapFunctionPrimitiveDecoder[Scala, Scala2, Driver](
    original: CqlPrimitiveDecoder.WithDriver[Scala, Driver],
    f: Scala => Scala2
  ) extends CqlPrimitiveDecoder[Scala2] {
    override type DriverType = original.DriverType
    override def driverClass: Class[DriverType] = original.driverClass
    override def driver2Scala(driverValue: DriverType, dataType: DataType): Scala2 =
      f(original.driver2Scala(driverValue, dataType))
  }
}
