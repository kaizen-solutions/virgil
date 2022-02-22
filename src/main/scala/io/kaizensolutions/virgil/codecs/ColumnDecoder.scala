package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{CqlDuration, GettableByName, TupleValue, UdtValue}
import magnolia1.{CaseClass, Magnolia}

import java.util
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

/**
 * Reader for Cassandra data types.
 * @see
 *   https://docs.datastax.com/en/developer/java-driver/4.11/manual/core/#cql-to-java-type-mapping
 * @tparam ScalaType
 */
@implicitNotFound(
  "No ColumnDecoder found for ${ScalaType}, please use Decoder.derive for a top level (Row) decoder and ColumnDecoder.deriveUdtValue for a User Defined Type decoder"
)
trait ColumnDecoder[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertDriverToScala(driverValue: DriverType): ScalaType

  def readFieldFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType

  def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): DriverType

  final def decodeFieldByName[Structure <: GettableByName](structure: Structure, fieldName: String): ScalaType =
    convertDriverToScala(driverValue = readFieldFromDriver(structure, fieldName))

  final def decodeFieldByIndex[Structure <: GettableByName](structure: Structure, fieldIndex: Int): ScalaType =
    convertDriverToScala(driverValue = readIndexFromDriver(structure, fieldIndex))

  final def map[ScalaType2](f: ScalaType => ScalaType2): ColumnDecoder.WithDriver[ScalaType2, DriverType] =
    new ColumnDecoder[ScalaType2] {
      override type DriverType = self.DriverType

      override def driverClass: Class[DriverType] = self.driverClass

      override def convertDriverToScala(driverValue: DriverType): ScalaType2 =
        f(self.convertDriverToScala(driverValue))

      def readFieldFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType =
        self.readFieldFromDriver(structure, fieldName)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): ColumnDecoder.this.DriverType =
        self.readIndexFromDriver(structure, index)
    }

  final def orDie[L, R](implicit ev: ScalaType =:= Either[L, R]): ColumnDecoder.WithDriver[R, DriverType] =
    self.map { in =>
      ev(in) match {
        case Left(error)  => throw new RuntimeException(s"Failed to map input to ${driverClass.getName}: $error")
        case Right(value) => value
      }
    }
}
object ColumnDecoder extends UdtColumnDecoderMagnoliaDerivation {
  type WithDriver[Scala, Driver] = ColumnDecoder[Scala] { type DriverType = Driver }

  def apply[A](implicit ev: ColumnDecoder[A]): ColumnDecoder[A] = ev

  def make[Scala, Driver](
    driverClassInfo: Class[Driver]
  )(convert: Driver => Scala)(
    fieldFn: (GettableByName, String) => Driver
  )(indexFn: (GettableByName, Int) => Driver): ColumnDecoder.WithDriver[Scala, Driver] =
    new ColumnDecoder[Scala] {
      override type DriverType = Driver

      override def driverClass: Class[DriverType] = driverClassInfo

      override def convertDriverToScala(driverValue: Driver): Scala = convert(driverValue)

      override def readFieldFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType = fieldFn(structure, fieldName)

      override def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): Driver =
        indexFn(structure, index)
    }

  def fromUdtValue[A](f: UdtValue => A): ColumnDecoder.WithDriver[A, UdtValue] = new ColumnDecoder[A] {
    override type DriverType = UdtValue

    override def driverClass: Class[DriverType] = classOf[UdtValue]

    override def convertDriverToScala(driverValue: DriverType): A = f(driverValue)

    override def readFieldFromDriver[Structure <: GettableByName](
      structure: Structure,
      fieldName: String
    ): DriverType = structure.getUdtValue(fieldName)

    override def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): UdtValue =
      structure.getUdtValue(index)
  }

  def fromRow[A](f: Row => A): ColumnDecoder.WithDriver[A, Row] = new ColumnDecoder[A] {
    override type DriverType = Row

    override def driverClass: Class[DriverType] = classOf[Row]

    override def convertDriverToScala(driverValue: DriverType): A = f(driverValue)

    override def readFieldFromDriver[Structure <: GettableByName](
      structure: Structure,
      fieldName: String
    ): DriverType = structure.asInstanceOf[Row]

    override def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): Row =
      structure.asInstanceOf[Row]
  }

  implicit val bigDecimalColumnDecoder: ColumnDecoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    make(classOf[java.math.BigDecimal])(BigDecimal.javaBigDecimal2bigDecimal)((structure, columnName) =>
      structure.getBigDecimal(columnName)
    )((structure, index) => structure.getBigDecimal(index))

  implicit val bigIntColumnDecoder: ColumnDecoder.WithDriver[BigInt, java.math.BigInteger] =
    make(classOf[java.math.BigInteger])(BigInt.javaBigInteger2bigInt)((structure, columnName) =>
      structure.getBigInteger(columnName)
    )((structure, index) => structure.getBigInteger(index))

  implicit val booleanColumnDecoder: ColumnDecoder.WithDriver[Boolean, java.lang.Boolean] =
    make(classOf[java.lang.Boolean])(Boolean.unbox)((structure, columnName) => structure.getBoolean(columnName))(
      (structure, index) => structure.getBoolean(index)
    )

  implicit val byteBufferColumnDecoder: ColumnDecoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    make(classOf[java.nio.ByteBuffer])(identity)((structure, columnName) => structure.getByteBuffer(columnName))(
      (structure, index) => structure.getByteBuffer(index)
    )

  implicit val byteColumnDecoder: ColumnDecoder.WithDriver[Byte, java.lang.Byte] =
    make(classOf[java.lang.Byte])(Byte.unbox)((structure, columnName) => structure.getByte(columnName))(
      (structure, index) => structure.getByte(index)
    )

  implicit val cqlTupleValueColumnDecoder: ColumnDecoder.WithDriver[TupleValue, TupleValue] =
    make(classOf[TupleValue])(identity)((structure, columnName) => structure.getTupleValue(columnName))(
      (structure, index) => structure.getTupleValue(index)
    )

  implicit val doubleColumnDecoder: ColumnDecoder.WithDriver[Double, java.lang.Double] =
    make(classOf[java.lang.Double])(Double.unbox)((structure, columnName) => structure.getDouble(columnName))(
      (structure, index) => structure.getDouble(index)
    )

  implicit val cqlDurationColumnDecoder: ColumnDecoder.WithDriver[CqlDuration, CqlDuration] =
    make(classOf[CqlDuration])(identity)((structure, columnName) => structure.getCqlDuration(columnName))(
      (structure, index) => structure.getCqlDuration(index)
    )

  implicit val floatColumnDecoder: ColumnDecoder.WithDriver[Float, java.lang.Float] =
    make(classOf[java.lang.Float])(Float.unbox)((structure, columnName) => structure.getFloat(columnName))(
      (structure, index) => structure.getFloat(index)
    )

  implicit val inetAddressColumnDecoder: ColumnDecoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    make(classOf[java.net.InetAddress])(identity)((structure, columnName) => structure.getInetAddress(columnName))(
      (structure, index) => structure.getInetAddress(index)
    )

  implicit val instantColumnDecoder: ColumnDecoder.WithDriver[java.time.Instant, java.time.Instant] =
    make(classOf[java.time.Instant])(identity)((structure, columnName) => structure.getInstant(columnName))(
      (structure, index) => structure.getInstant(index)
    )

  implicit val intColumnDecoder: ColumnDecoder.WithDriver[Int, Integer] =
    make(classOf[java.lang.Integer])(Int.unbox)((structure, columnName) => structure.getInt(columnName))(
      (structure, index) => structure.getInt(index)
    )

  implicit val localDateColumnDecoder: ColumnDecoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    make(classOf[java.time.LocalDate])(identity)((structure, columnName) => structure.getLocalDate(columnName))(
      (structure, index) => structure.getLocalDate(index)
    )

  implicit val localTimeColumnDecoder: ColumnDecoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    make(classOf[java.time.LocalTime])(identity)((structure, columnName) => structure.getLocalTime(columnName))(
      (structure, index) => structure.getLocalTime(index)
    )

  implicit val longColumnDecoder: ColumnDecoder.WithDriver[Long, java.lang.Long] =
    make(classOf[java.lang.Long])(Long.unbox)((structure, columnName) => structure.getLong(columnName))(
      (structure, index) => structure.getLong(index)
    )

  implicit val shortColumnDecoder: ColumnDecoder.WithDriver[Short, java.lang.Short] =
    make(classOf[java.lang.Short])(s => Short.unbox(s))((structure, columnName) => structure.getShort(columnName))(
      (structure, index) => structure.getShort(index)
    )

  implicit val stringColumnDecoder: ColumnDecoder.WithDriver[String, java.lang.String] =
    make(classOf[java.lang.String])(identity)((structure, columnName) => structure.getString(columnName))(
      (structure, index) => structure.getString(index)
    )

  implicit val uuidColumnDecoder: ColumnDecoder.WithDriver[java.util.UUID, java.util.UUID] =
    make(classOf[java.util.UUID])(identity)((structure, columnName) => structure.getUuid(columnName))(
      (structure, index) => structure.getUuid(index)
    )

  implicit val rowColumnDecoder: ColumnDecoder.WithDriver[Row, Row] =
    make(classOf[Row])(identity)((structure, _) => structure.asInstanceOf[Row])((structure, _) =>
      structure.asInstanceOf[Row]
    )

  implicit val udtValueDecoder: ColumnDecoder.WithDriver[UdtValue, UdtValue] =
    make(classOf[UdtValue])(identity)((structure, columnName) => structure.getUdtValue(columnName))(
      (structure, index) => structure.getUdtValue(index)
    )

  implicit def listColumnDecoder[A](implicit elementColumnDecoder: ColumnDecoder[A]): ColumnDecoder[List[A]] =
    new ColumnDecoder[List[A]] {
      override type DriverType = java.util.List[elementColumnDecoder.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.List[elementColumnDecoder.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.List[elementColumnDecoder.DriverType]
      ): List[A] =
        driverValue.asScala.map(elementColumnDecoder.convertDriverToScala).toList

      override def readFieldFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): java.util.List[elementColumnDecoder.DriverType] =
        structure.getList(fieldName, elementColumnDecoder.driverClass)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): util.List[elementColumnDecoder.DriverType] =
        structure.getList(index, elementColumnDecoder.driverClass)
    }

  implicit def setColumnDecoder[A](implicit elementColumnDecoder: ColumnDecoder[A]): ColumnDecoder[Set[A]] =
    new ColumnDecoder[Set[A]] {
      override type DriverType = java.util.Set[elementColumnDecoder.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.Set[elementColumnDecoder.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Set[elementColumnDecoder.DriverType]
      ): Set[A] =
        driverValue.asScala.map(elementColumnDecoder.convertDriverToScala(_)).toSet

      override def readFieldFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.getSet(fieldName, elementColumnDecoder.driverClass)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): util.Set[elementColumnDecoder.DriverType] =
        structure.getSet(index, elementColumnDecoder.driverClass)
    }

  implicit def mapColumnDecoder[K, V](implicit
    keyColumnDecoder: ColumnDecoder[K],
    valueColumnDecoder: ColumnDecoder[V]
  ): ColumnDecoder[Map[K, V]] =
    new ColumnDecoder[Map[K, V]] {
      override type DriverType = java.util.Map[keyColumnDecoder.DriverType, valueColumnDecoder.DriverType]

      override def driverClass: Class[DriverType] =
        classOf[java.util.Map[keyColumnDecoder.DriverType, valueColumnDecoder.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Map[keyColumnDecoder.DriverType, valueColumnDecoder.DriverType]
      ): Map[K, V] =
        driverValue.asScala.map { case (key, value) =>
          keyColumnDecoder.convertDriverToScala(key) -> valueColumnDecoder.convertDriverToScala(value)
        }.toMap

      override def readFieldFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.getMap(fieldName, keyColumnDecoder.driverClass, valueColumnDecoder.driverClass)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): util.Map[keyColumnDecoder.DriverType, valueColumnDecoder.DriverType] =
        structure.getMap(index, keyColumnDecoder.driverClass, valueColumnDecoder.driverClass)
    }

  implicit def optionColumnDecoder[A](implicit elementColumnDecoder: ColumnDecoder[A]): ColumnDecoder[Option[A]] =
    new ColumnDecoder[Option[A]] {
      override type DriverType = elementColumnDecoder.DriverType

      override def driverClass: Class[DriverType] = elementColumnDecoder.driverClass

      override def convertDriverToScala(driverValue: DriverType): Option[A] =
        Option(driverValue).map(elementColumnDecoder.convertDriverToScala(_))

      override def readFieldFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: String
      ): DriverType =
        structure.get(fieldName, elementColumnDecoder.driverClass)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): elementColumnDecoder.DriverType =
        structure.get(index, elementColumnDecoder.driverClass)
    }
}

trait UdtColumnDecoderMagnoliaDerivation {
  type Typeclass[T] = ColumnDecoder[T]

  // Automatic derivation of Reader instances for UDTs
  def join[T](ctx: CaseClass[ColumnDecoder, T]): ColumnDecoder.WithDriver[T, UdtValue] =
    ColumnDecoder.fromUdtValue { udtValue =>
      ctx.construct { param =>
        val fieldName = param.label
        val reader    = param.typeclass
        reader.decodeFieldByName(udtValue, fieldName)
      }
    }

  implicit def deriveUdtValue[T]: ColumnDecoder.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
