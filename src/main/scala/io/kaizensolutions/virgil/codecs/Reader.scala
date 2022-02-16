package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{CqlDuration, GettableByName, TupleValue, UdtValue}
import magnolia1.{CaseClass, Magnolia}

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

/**
 * Reader for Cassandra data types.
 * @see
 *   https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/#cql-to-java-type-mapping
 * @tparam ScalaType
 */
@implicitNotFound(
  "No Reader found for ${T}, please use RowReader.derive for a top level (Row) reader and UdtReader.derive for a User Defined Type"
)
trait Reader[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertDriverToScala(driverValue: DriverType): ScalaType

  def readFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType

  final def read[Structure <: GettableByName](structure: Structure, fieldName: String): ScalaType =
    convertDriverToScala(driverValue = readFromDriver(structure, fieldName))

  def map[ScalaType2](f: ScalaType => ScalaType2): Reader[ScalaType2] = new Reader[ScalaType2] {
    override type DriverType = self.DriverType

    override def driverClass: Class[DriverType] = self.driverClass

    override def convertDriverToScala(driverValue: DriverType): ScalaType2 =
      f(self.convertDriverToScala(driverValue))

    def readFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType =
      self.readFromDriver(structure, fieldName)
  }
}
object Reader extends UdtReaderMagnoliaDerivation {
  type WithDriver[Scala, Driver] = Reader[Scala] { type DriverType = Driver }

  def make[Scala, Driver](
    driverClassInfo: Class[Driver]
  )(convert: Driver => Scala)(fn: (GettableByName, String) => Driver): Reader.WithDriver[Scala, Driver] =
    new Reader[Scala] {
      override type DriverType = Driver

      override def driverClass: Class[DriverType] = driverClassInfo

      override def convertDriverToScala(driverValue: Driver): Scala = convert(driverValue)

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType = fn(structure, fieldName)
    }

  def fromUdtValue[A](f: UdtValue => A): Reader.WithDriver[A, UdtValue] = new Reader[A] {
    override type DriverType = UdtValue

    override def driverClass: Class[DriverType] = classOf[UdtValue]

    override def convertDriverToScala(driverValue: DriverType): A = f(driverValue)

    override def readFromDriver[Structure <: GettableByName](
      structure: Structure,
      fieldName: String
    ): DriverType = structure.getUdtValue(fieldName)
  }

  def fromRow[A](f: Row => A): Reader.WithDriver[A, Row] = new Reader[A] {
    override type DriverType = Row

    override def driverClass: Class[DriverType] = classOf[Row]

    override def convertDriverToScala(driverValue: DriverType): A = f(driverValue)

    override def readFromDriver[Structure <: GettableByName](
      structure: Structure,
      fieldName: String
    ): DriverType = structure.asInstanceOf[Row]
  }

  implicit val bigDecimalReader: Reader.WithDriver[BigDecimal, java.math.BigDecimal] =
    make(classOf[java.math.BigDecimal])(BigDecimal.javaBigDecimal2bigDecimal)((structure, columnName) =>
      structure.getBigDecimal(columnName)
    )

  implicit val bigIntReader: Reader.WithDriver[BigInt, java.math.BigInteger] =
    make(classOf[java.math.BigInteger])(BigInt.javaBigInteger2bigInt)((structure, columnName) =>
      structure.getBigInteger(columnName)
    )

  implicit val booleanReader: Reader.WithDriver[Boolean, java.lang.Boolean] =
    make(classOf[java.lang.Boolean])(Boolean.unbox)((structure, columnName) => structure.getBoolean(columnName))

  implicit val byteBufferReader: Reader.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    make(classOf[java.nio.ByteBuffer])(identity)((structure, columnName) => structure.getByteBuffer(columnName))

  implicit val byteReader: Reader.WithDriver[Byte, java.lang.Byte] =
    make(classOf[java.lang.Byte])(Byte.unbox)((structure, columnName) => structure.getByte(columnName))

  implicit val cqlTupleValueReader: Reader.WithDriver[TupleValue, TupleValue] =
    make(classOf[TupleValue])(identity)((structure, columnName) => structure.getTupleValue(columnName))

  implicit val doubleReader: Reader.WithDriver[Double, java.lang.Double] =
    make(classOf[java.lang.Double])(Double.unbox)((structure, columnName) => structure.getDouble(columnName))

  implicit val cqlDurationReader: Reader.WithDriver[CqlDuration, CqlDuration] =
    make(classOf[CqlDuration])(identity)((structure, columnName) => structure.getCqlDuration(columnName))

  implicit val floatReader: Reader.WithDriver[Float, java.lang.Float] =
    make(classOf[java.lang.Float])(Float.unbox)((structure, columnName) => structure.getFloat(columnName))

  implicit val inetAddressReader: Reader.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    make(classOf[java.net.InetAddress])(identity)((structure, columnName) => structure.getInetAddress(columnName))

  implicit val instantReader: Reader.WithDriver[java.time.Instant, java.time.Instant] =
    make(classOf[java.time.Instant])(identity)((structure, columnName) => structure.getInstant(columnName))

  implicit val intReader: Reader.WithDriver[Int, Integer] =
    make(classOf[java.lang.Integer])(Int.unbox)((structure, columnName) => structure.getInt(columnName))

  implicit val localDateReader: Reader.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    make(classOf[java.time.LocalDate])(identity)((structure, columnName) => structure.getLocalDate(columnName))

  implicit val localTimeReader: Reader.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    make(classOf[java.time.LocalTime])(identity)((structure, columnName) => structure.getLocalTime(columnName))

  implicit val longReader: Reader.WithDriver[Long, java.lang.Long] =
    make(classOf[java.lang.Long])(Long.unbox)((structure, columnName) => structure.getLong(columnName))

  implicit val shortReader: Reader.WithDriver[Short, java.lang.Short] =
    make(classOf[java.lang.Short])(s => Short.unbox(s))((structure, columnName) => structure.getShort(columnName))

  implicit val stringReader: Reader.WithDriver[String, java.lang.String] =
    make(classOf[java.lang.String])(identity)((structure, columnName) => structure.getString(columnName))

  implicit val uuidReader: Reader.WithDriver[java.util.UUID, java.util.UUID] =
    make(classOf[java.util.UUID])(identity)((structure, columnName) => structure.getUuid(columnName))

  implicit val rowReader: Reader.WithDriver[Row, Row] =
    make(classOf[Row])(identity)((structure, _) => structure.asInstanceOf[Row])

  implicit def listReader[A](implicit elementReader: Reader[A]): Reader[List[A]] =
    new Reader[List[A]] {
      override type DriverType = java.util.List[elementReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.List[elementReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.List[elementReader.DriverType]
      ): List[A] =
        driverValue.asScala.map(elementReader.convertDriverToScala(_)).toList

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): java.util.List[elementReader.DriverType] =
        structure.getList(fieldName, elementReader.driverClass)
    }

  implicit def setReader[A](implicit elementReader: Reader[A]): Reader[Set[A]] =
    new Reader[Set[A]] {
      override type DriverType = java.util.Set[elementReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.Set[elementReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Set[elementReader.DriverType]
      ): Set[A] =
        driverValue.asScala.map(elementReader.convertDriverToScala(_)).toSet

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.getSet(fieldName, elementReader.driverClass)
    }

  implicit def mapReader[K, V](implicit
    keyReader: Reader[K],
    valueReader: Reader[V]
  ): Reader[Map[K, V]] =
    new Reader[Map[K, V]] {
      override type DriverType = java.util.Map[keyReader.DriverType, valueReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.Map[keyReader.DriverType, valueReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Map[keyReader.DriverType, valueReader.DriverType]
      ): Map[K, V] =
        driverValue.asScala.map { case (key, value) =>
          keyReader.convertDriverToScala(key) -> valueReader.convertDriverToScala(value)
        }.toMap

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.getMap(fieldName, keyReader.driverClass, valueReader.driverClass)
    }

  implicit def optionReader[A](implicit elementReader: Reader[A]): Reader[Option[A]] =
    new Reader[Option[A]] {
      override type DriverType = elementReader.DriverType

      override def driverClass: Class[DriverType] = elementReader.driverClass

      override def convertDriverToScala(driverValue: DriverType): Option[A] =
        Option(driverValue).map(elementReader.convertDriverToScala(_))

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.get(fieldName, elementReader.driverClass)
    }
}

trait UdtReaderMagnoliaDerivation {
  type Typeclass[T] = Reader[T]

  // Automatic derivation of Reader instances for UDTs
  def join[T](ctx: CaseClass[Reader, T]): Reader.WithDriver[T, UdtValue] =
    Reader.fromUdtValue { udtValue =>
      ctx.construct { param =>
        val fieldName = param.label
        val reader    = param.typeclass
        reader.read(udtValue, fieldName)
      }
    }

  implicit def deriveUdtValue[T]: Reader.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
