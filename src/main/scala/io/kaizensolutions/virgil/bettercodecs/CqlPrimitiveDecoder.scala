package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.`type`.{DataType, ListType, MapType, SetType}
import com.datastax.oss.driver.api.core.data.{CqlDuration, UdtValue}

import scala.jdk.CollectionConverters._

trait CqlPrimitiveDecoder[ScalaType] { self =>
  type DriverType
  def driverClass: Class[DriverType]
  def driver2Scala(driverValue: DriverType, dataType: DataType): ScalaType

  def map[ScalaType2](f: ScalaType => ScalaType2): CqlPrimitiveDecoder[ScalaType2] =
    new CqlPrimitiveDecoder[ScalaType2] {
      override type DriverType = self.DriverType
      override def driverClass: Class[DriverType] = self.driverClass
      override def driver2Scala(driverValue: DriverType, dataType: DataType): ScalaType2 =
        f(self.driver2Scala(driverValue, dataType))
    }
}
object CqlPrimitiveDecoder {
  type WithDriver[Scala, Driver] = CqlPrimitiveDecoder[Scala] { type DriverType = Driver }

  implicit val stringPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[String, java.lang.String] =
    new CqlPrimitiveDecoder[String] {
      type DriverType = java.lang.String
      def driverClass: Class[DriverType]                                    = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): String = driverValue
    }
  implicit val bigIntPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[BigInt, java.math.BigInteger] =
    new CqlPrimitiveDecoder[BigInt] {
      type DriverType = java.math.BigInteger
      def driverClass: Class[DriverType]                                    = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): BigInt = BigInt(driverValue)
    }
  implicit val byteBufferPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    new CqlPrimitiveDecoder[java.nio.ByteBuffer] {
      type DriverType = java.nio.ByteBuffer
      def driverClass: Class[DriverType]                                                 = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.nio.ByteBuffer = driverValue
    }

  implicit val booleanPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Boolean, java.lang.Boolean] =
    new CqlPrimitiveDecoder[Boolean] {
      type DriverType = java.lang.Boolean
      def driverClass: Class[DriverType]                                     = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Boolean = driverValue
    }

  implicit val longPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Long, java.lang.Long] =
    new CqlPrimitiveDecoder[Long] {
      type DriverType = java.lang.Long
      def driverClass: Class[DriverType]                                  = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Long = driverValue
    }

  implicit val datePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    new CqlPrimitiveDecoder[java.time.LocalDate] {
      type DriverType = java.time.LocalDate
      def driverClass: Class[DriverType]                                                 = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.LocalDate = driverValue
    }

  implicit val bigDecimalPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    new CqlPrimitiveDecoder[BigDecimal] {
      type DriverType = java.math.BigDecimal
      def driverClass: Class[DriverType]                                        = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): BigDecimal = BigDecimal(driverValue)
    }

  implicit val doublePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Double, java.lang.Double] =
    new CqlPrimitiveDecoder[Double] {
      type DriverType = java.lang.Double
      def driverClass: Class[DriverType]                                    = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Double = driverValue
    }

  implicit val cqlDurationPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[CqlDuration, CqlDuration] =
    new CqlPrimitiveDecoder[CqlDuration] {
      type DriverType = CqlDuration
      def driverClass: Class[DriverType]                                         = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): CqlDuration = driverValue
    }

  implicit val floatPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Float, java.lang.Float] =
    new CqlPrimitiveDecoder[Float] {
      type DriverType = java.lang.Float
      def driverClass: Class[DriverType]                                   = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Float = driverValue
    }

  implicit val inetPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    new CqlPrimitiveDecoder[java.net.InetAddress] {
      type DriverType = java.net.InetAddress
      def driverClass: Class[DriverType]                                                  = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.net.InetAddress = driverValue
    }

  implicit val intPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Int, java.lang.Integer] =
    new CqlPrimitiveDecoder[Int] {
      type DriverType = java.lang.Integer
      def driverClass: Class[DriverType]                                 = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Int = driverValue
    }

  implicit val shortPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Short, java.lang.Short] =
    new CqlPrimitiveDecoder[Short] {
      type DriverType = java.lang.Short
      def driverClass: Class[DriverType]                                   = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Short = driverValue
    }

  implicit val localTimePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    new CqlPrimitiveDecoder[java.time.LocalTime] {
      type DriverType = java.time.LocalTime
      def driverClass: Class[DriverType]                                                 = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.LocalTime = driverValue
    }

  implicit val instantPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.time.Instant, java.time.Instant] =
    new CqlPrimitiveDecoder[java.time.Instant] {
      type DriverType = java.time.Instant
      def driverClass: Class[DriverType]                                               = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.time.Instant = driverValue
    }

  implicit val uuidPrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[java.util.UUID, java.util.UUID] =
    new CqlPrimitiveDecoder[java.util.UUID] {
      type DriverType = java.util.UUID
      def driverClass: Class[DriverType]                                            = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): java.util.UUID = driverValue
    }

  implicit val bytePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[Byte, java.lang.Byte] =
    new CqlPrimitiveDecoder[Byte] {
      type DriverType = java.lang.Byte
      def driverClass: Class[DriverType]                                  = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): Byte = driverValue
    }

  implicit val udtValuePrimitiveDecoder: CqlPrimitiveDecoder.WithDriver[UdtValue, UdtValue] =
    new CqlPrimitiveDecoder[UdtValue] {
      type DriverType = UdtValue
      def driverClass: Class[DriverType]                                      = classOf[DriverType]
      def driver2Scala(driverValue: DriverType, dataType: DataType): UdtValue = driverValue
    }

  implicit def scalaTypeViaUdtValuePrimitive[A](implicit decoder: UdtValueDecoder.Object[A]): CqlPrimitiveDecoder[A] =
    new CqlPrimitiveDecoder[A] {
      type DriverType = UdtValue
      def driverClass: Class[DriverType] = classOf[DriverType]

      def driver2Scala(driverValue: DriverType, dataType: DataType): A =
        decoder.decode(driverValue)
    }

  implicit def listCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[List[A], java.util.List[element.DriverType]] = new CqlPrimitiveDecoder[List[A]] {
    override type DriverType = java.util.List[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def driver2Scala(driverValue: DriverType, dataType: DataType): List[A] = {
      val elementDataType = dataType.asInstanceOf[ListType].getElementType
      driverValue.asScala.map(element.driver2Scala(_, elementDataType)).toList
    }
  }

  implicit def setCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[Set[A], java.util.Set[element.DriverType]] = new CqlPrimitiveDecoder[Set[A]] {
    override type DriverType = java.util.Set[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def driver2Scala(driverValue: DriverType, dataType: DataType): Set[A] = {
      val elementDataType = dataType.asInstanceOf[SetType].getElementType
      driverValue.asScala.map(element.driver2Scala(_, elementDataType)).toSet
    }
  }

  implicit def mapCqlPrimitiveDecoder[A, B](implicit
    key: CqlPrimitiveDecoder[A],
    value: CqlPrimitiveDecoder[B]
  ): CqlPrimitiveDecoder.WithDriver[Map[A, B], java.util.Map[key.DriverType, value.DriverType]] =
    new CqlPrimitiveDecoder[Map[A, B]] {
      override type DriverType = java.util.Map[key.DriverType, value.DriverType]
      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def driver2Scala(driverValue: DriverType, dataType: DataType): Map[A, B] = {
        val mapType       = dataType.asInstanceOf[MapType]
        val keyDataType   = mapType.getKeyType
        val valueDataType = mapType.getValueType
        driverValue.asScala.map { case (k, v) =>
          key.driver2Scala(k, keyDataType) -> value.driver2Scala(v, valueDataType)
        }.toMap
      }
    }

  implicit def optionCqlPrimitiveDecoder[A](implicit element: CqlPrimitiveDecoder[A]): CqlPrimitiveDecoder[Option[A]] =
    new CqlPrimitiveDecoder[Option[A]] {
      override type DriverType = element.DriverType
      override def driverClass: Class[DriverType] = element.driverClass

      override def driver2Scala(driverValue: DriverType, dataType: DataType): Option[A] =
        Option(driverValue).map(element.driver2Scala(_, dataType))
    }
}
