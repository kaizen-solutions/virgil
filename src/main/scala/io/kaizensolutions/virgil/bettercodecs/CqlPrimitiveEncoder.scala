package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.data.{CqlDuration, UdtValue}

import scala.jdk.CollectionConverters._

trait CqlPrimitiveEncoder[ScalaType] { self =>
  type DriverType
  def driverClass: Class[DriverType]
  def scala2Driver(scalaValue: ScalaType, dataType: DataType): DriverType

  def contramap[ScalaType2](
    f: ScalaType2 => ScalaType
  ): CqlPrimitiveEncoder.WithDriver[ScalaType2, DriverType] =
    new CqlPrimitiveEncoder[ScalaType2] {
      override type DriverType = self.DriverType

      override def driverClass: Class[DriverType] = self.driverClass

      override def scala2Driver(scalaValue: ScalaType2, dataType: DataType): DriverType =
        self.scala2Driver(f(scalaValue), dataType)
    }
}
object CqlPrimitiveEncoder {
  type WithDriver[Scala, Driver] = CqlPrimitiveEncoder[Scala] { type DriverType = Driver }

  implicit val stringPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[String, java.lang.String] =
    new CqlPrimitiveEncoder[String] {
      type DriverType = java.lang.String
      def driverClass: Class[DriverType]                                   = classOf[DriverType]
      def scala2Driver(scalaValue: String, dataType: DataType): DriverType = scalaValue
    }
  implicit val bigIntPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[BigInt, java.math.BigInteger] =
    new CqlPrimitiveEncoder[BigInt] {
      type DriverType = java.math.BigInteger
      def driverClass: Class[DriverType]                                   = classOf[DriverType]
      def scala2Driver(scalaValue: BigInt, dataType: DataType): DriverType = scalaValue.bigInteger
    }
  implicit val byteBufferPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    new CqlPrimitiveEncoder[java.nio.ByteBuffer] {
      type DriverType = java.nio.ByteBuffer
      def driverClass: Class[DriverType]                                                = classOf[DriverType]
      def scala2Driver(scalaValue: java.nio.ByteBuffer, dataType: DataType): DriverType = scalaValue
    }

  implicit val booleanPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Boolean, java.lang.Boolean] =
    new CqlPrimitiveEncoder[Boolean] {
      type DriverType = java.lang.Boolean
      def driverClass: Class[DriverType]                                    = classOf[DriverType]
      def scala2Driver(scalaValue: Boolean, dataType: DataType): DriverType = scalaValue
    }

  implicit val longPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Long, java.lang.Long] =
    new CqlPrimitiveEncoder[Long] {
      type DriverType = java.lang.Long
      def driverClass: Class[DriverType]                                 = classOf[DriverType]
      def scala2Driver(scalaValue: Long, dataType: DataType): DriverType = scalaValue
    }

  implicit val datePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    new CqlPrimitiveEncoder[java.time.LocalDate] {
      type DriverType = java.time.LocalDate
      def driverClass: Class[DriverType]                                                = classOf[DriverType]
      def scala2Driver(scalaValue: java.time.LocalDate, dataType: DataType): DriverType = scalaValue
    }

  implicit val bigDecimalPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    new CqlPrimitiveEncoder[BigDecimal] {
      type DriverType = java.math.BigDecimal
      def driverClass: Class[DriverType]                                       = classOf[DriverType]
      def scala2Driver(scalaValue: BigDecimal, dataType: DataType): DriverType = scalaValue.bigDecimal
    }

  implicit val doublePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Double, java.lang.Double] =
    new CqlPrimitiveEncoder[Double] {
      type DriverType = java.lang.Double
      def driverClass: Class[DriverType]                                   = classOf[DriverType]
      def scala2Driver(scalaValue: Double, dataType: DataType): DriverType = scalaValue
    }

  implicit val cqlDurationPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[CqlDuration, CqlDuration] =
    new CqlPrimitiveEncoder[CqlDuration] {
      type DriverType = CqlDuration
      def driverClass: Class[DriverType]                                        = classOf[DriverType]
      def scala2Driver(scalaValue: CqlDuration, dataType: DataType): DriverType = scalaValue
    }

  implicit val floatPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Float, java.lang.Float] =
    new CqlPrimitiveEncoder[Float] {
      type DriverType = java.lang.Float
      def driverClass: Class[DriverType]                                  = classOf[DriverType]
      def scala2Driver(scalaValue: Float, dataType: DataType): DriverType = scalaValue
    }

  implicit val inetPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    new CqlPrimitiveEncoder[java.net.InetAddress] {
      type DriverType = java.net.InetAddress
      def driverClass: Class[DriverType]                                                 = classOf[DriverType]
      def scala2Driver(scalaValue: java.net.InetAddress, dataType: DataType): DriverType = scalaValue
    }

  implicit val intPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Int, java.lang.Integer] =
    new CqlPrimitiveEncoder[Int] {
      type DriverType = java.lang.Integer
      def driverClass: Class[DriverType]                                = classOf[DriverType]
      def scala2Driver(scalaValue: Int, dataType: DataType): DriverType = scalaValue
    }

  implicit val shortPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Short, java.lang.Short] =
    new CqlPrimitiveEncoder[Short] {
      type DriverType = java.lang.Short
      def driverClass: Class[DriverType]                                  = classOf[DriverType]
      def scala2Driver(scalaValue: Short, dataType: DataType): DriverType = scalaValue
    }

  implicit val localTimePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    new CqlPrimitiveEncoder[java.time.LocalTime] {
      type DriverType = java.time.LocalTime
      def driverClass: Class[DriverType]                                                = classOf[DriverType]
      def scala2Driver(scalaValue: java.time.LocalTime, dataType: DataType): DriverType = scalaValue
    }

  implicit val instantPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.time.Instant, java.time.Instant] =
    new CqlPrimitiveEncoder[java.time.Instant] {
      type DriverType = java.time.Instant
      def driverClass: Class[DriverType]                                              = classOf[DriverType]
      def scala2Driver(scalaValue: java.time.Instant, dataType: DataType): DriverType = scalaValue
    }

  implicit val uuidPrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[java.util.UUID, java.util.UUID] =
    new CqlPrimitiveEncoder[java.util.UUID] {
      type DriverType = java.util.UUID
      def driverClass: Class[DriverType]                                           = classOf[DriverType]
      def scala2Driver(scalaValue: java.util.UUID, dataType: DataType): DriverType = scalaValue
    }

  implicit val bytePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[Byte, java.lang.Byte] =
    new CqlPrimitiveEncoder[Byte] {
      type DriverType = java.lang.Byte
      def driverClass: Class[DriverType]                                 = classOf[DriverType]
      def scala2Driver(scalaValue: Byte, dataType: DataType): DriverType = scalaValue
    }

  implicit val udtValuePrimitiveEncoder: CqlPrimitiveEncoder.WithDriver[UdtValue, UdtValue] =
    new CqlPrimitiveEncoder[UdtValue] {
      type DriverType = UdtValue
      def driverClass: Class[DriverType]                                     = classOf[DriverType]
      def scala2Driver(scalaValue: UdtValue, dataType: DataType): DriverType = scalaValue
    }

  implicit def scalaTypeViaUdtValuePrimitive[A](implicit
    encoder: UdtValueEncoder.Object[A]
  ): CqlPrimitiveEncoder[A] =
    new CqlPrimitiveEncoder[A] {
      type DriverType = UdtValue
      def driverClass: Class[DriverType] = classOf[DriverType]
      def scala2Driver(scalaValue: A, dataType: DataType): DriverType = {
        val udtValue = dataType.asInstanceOf[UserDefinedType].newValue()
        encoder.encode(udtValue, scalaValue)
      }
    }

  implicit def listCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[List[A], java.util.List[element.DriverType]] = new CqlPrimitiveEncoder[List[A]] {
    override type DriverType = java.util.List[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def scala2Driver(scalaValue: List[A], dataType: DataType): DriverType = {
      val elementDataType = dataType.asInstanceOf[ListType].getElementType
      scalaValue.map(element.scala2Driver(_, elementDataType)).asJava
    }

  }

  implicit def setCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): CqlPrimitiveEncoder.WithDriver[Set[A], java.util.Set[element.DriverType]] = new CqlPrimitiveEncoder[Set[A]] {
    override type DriverType = java.util.Set[element.DriverType]
    override def driverClass: Class[DriverType] = classOf[DriverType]
    override def scala2Driver(scalaValue: Set[A], dataType: DataType): DriverType = {
      val elementDataType = dataType.asInstanceOf[SetType].getElementType
      scalaValue.map(element.scala2Driver(_, elementDataType)).asJava
    }

  }

  implicit def mapCqlPrimitiveEncoder[A, B](implicit
    key: CqlPrimitiveEncoder[A],
    value: CqlPrimitiveEncoder[B]
  ): CqlPrimitiveEncoder.WithDriver[Map[A, B], java.util.Map[key.DriverType, value.DriverType]] =
    new CqlPrimitiveEncoder[Map[A, B]] {
      override type DriverType = java.util.Map[key.DriverType, value.DriverType]
      override def driverClass: Class[DriverType] = classOf[DriverType]
      override def scala2Driver(scalaValue: Map[A, B], dataType: DataType): DriverType = {
        val mapType       = dataType.asInstanceOf[MapType]
        val keyDataType   = mapType.getKeyType
        val valueDataType = mapType.getValueType
        scalaValue.map { case (k, v) =>
          key.scala2Driver(k, keyDataType) -> value.scala2Driver(v, valueDataType)
        }.asJava
      }

    }

  implicit def optionCqlPrimitiveEncoder[A](implicit element: CqlPrimitiveEncoder[A]): CqlPrimitiveEncoder[Option[A]] =
    new CqlPrimitiveEncoder[Option[A]] {
      override type DriverType = element.DriverType
      override def driverClass: Class[DriverType] = element.driverClass
      override def scala2Driver(scalaValue: Option[A], dataType: DataType): DriverType =
        scalaValue match {
          case Some(value) => element.scala2Driver(value, dataType)
          case None        => null.asInstanceOf[DriverType]
        }
    }
}
