package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{GettableByName => CassandraStructure, UdtValue}
import magnolia1.{CaseClass, Magnolia}

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

@implicitNotFound(
  "No Reader found for ${T}, please use RowReader.derive for a top level (Row) reader and UdtReader.derive for a User Defined Type"
)
trait Reader[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertDriverToScala(driverValue: DriverType, dataType: DataType): ScalaType

  def readFromDriver[Structure <: CassandraStructure](structure: Structure, fieldName: Option[String]): DriverType

  final def read[Structure <: CassandraStructure](structure: Structure, fieldName: Option[String]): ScalaType =
    convertDriverToScala(
      driverValue = readFromDriver(structure, fieldName),
      dataType = fieldName.fold(DataTypes.custom("fieldName-not-provided"))(structure.getType)
    )

  def map[ScalaType2](f: ScalaType => ScalaType2): Reader[ScalaType2] = new Reader[ScalaType2] {
    override type DriverType = self.DriverType

    override def driverClass: Class[DriverType] = self.driverClass

    override def convertDriverToScala(driverValue: DriverType, dataType: DataType): ScalaType2 =
      f(self.convertDriverToScala(driverValue, dataType))

    def readFromDriver[Structure <: CassandraStructure](structure: Structure, fieldName: Option[String]): DriverType =
      self.readFromDriver(structure, fieldName)
  }
}
object Reader extends UdtReaderMagnoliaDerivation {
  type WithDriver[Scala, Driver] = Reader[Scala] { type DriverType = Driver }

  def make[Scala, Driver](
    driverClassInfo: Class[Driver]
  )(
    convert: (Driver, DataType) => Scala
  )(fn: (CassandraStructure, String) => Driver): Reader.WithDriver[Scala, Driver] =
    new Reader[Scala] {
      override type DriverType = Driver

      override def driverClass: Class[DriverType] = driverClassInfo

      override def convertDriverToScala(driverValue: Driver, dataType: DataType): Scala = convert(driverValue, dataType)

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        fieldName match {
          case Some(fieldName) => fn(structure, fieldName)
          case None            => throw new RuntimeException("Expected a field name to extract but was not provided one")
        }
    }

  implicit val bigDecimalReader: Reader.WithDriver[BigDecimal, java.math.BigDecimal] =
    make(classOf[java.math.BigDecimal])((d, _) => BigDecimal.javaBigDecimal2bigDecimal(d))((structure, columnName) =>
      structure.getBigDecimal(columnName)
    )

  implicit val bigIntReader: Reader.WithDriver[BigInt, java.math.BigInteger] =
    make(classOf[java.math.BigInteger])((b, _) => BigInt.javaBigInteger2bigInt(b))((structure, columnName) =>
      structure.getBigInteger(columnName)
    )

  implicit val booleanReader: Reader.WithDriver[Boolean, java.lang.Boolean] =
    make(classOf[java.lang.Boolean])((b, _) => Boolean.unbox(b))((structure, columnName) =>
      structure.getBoolean(columnName)
    )

  implicit val byteBufferReader: Reader.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    make(classOf[java.nio.ByteBuffer])(identityDatatype)((structure, columnName) => structure.getByteBuffer(columnName))

  implicit val byteReader: Reader.WithDriver[Byte, java.lang.Byte] =
    make(classOf[java.lang.Byte])((b, _) => Byte.unbox(b))((structure, columnName) => structure.getByte(columnName))

  implicit val doubleReader: Reader.WithDriver[Double, java.lang.Double] =
    make(classOf[java.lang.Double])((d, _) => Double.unbox(d))((structure, columnName) =>
      structure.getDouble(columnName)
    )

  implicit val instantReader: Reader.WithDriver[java.time.Instant, java.time.Instant] =
    make(classOf[java.time.Instant])(identityDatatype)((structure, columnName) => structure.getInstant(columnName))

  implicit val intReader: Reader.WithDriver[Int, Integer] =
    make(classOf[java.lang.Integer])((i, _) => Int.unbox(i))((structure, columnName) => structure.getInt(columnName))

  implicit val localDateReader: Reader.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    make(classOf[java.time.LocalDate])(identityDatatype)((structure, columnName) => structure.getLocalDate(columnName))

  implicit val localTimeReader: Reader.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    make(classOf[java.time.LocalTime])(identityDatatype)((structure, columnName) => structure.getLocalTime(columnName))

  implicit val longReader: Reader.WithDriver[Long, java.lang.Long] =
    make(classOf[java.lang.Long])((l, _) => Long.unbox(l))((structure, columnName) => structure.getLong(columnName))

  implicit val shortReader: Reader.WithDriver[Short, java.lang.Short] =
    make(classOf[java.lang.Short])((s, _) => Short.unbox(s))((structure, columnName) => structure.getShort(columnName))

  implicit val stringReader: Reader.WithDriver[String, java.lang.String] =
    make(classOf[java.lang.String])(identityDatatype)((structure, columnName) => structure.getString(columnName))

  implicit val uuidReader: Reader.WithDriver[java.util.UUID, java.util.UUID] =
    make(classOf[java.util.UUID])(identityDatatype)((structure, columnName) => structure.getUuid(columnName))

  implicit val rowReader: Reader.WithDriver[Row, Row] =
    make(classOf[Row])(identityDatatype)((structure, _) => structure.asInstanceOf[Row])

  implicit def listReader[A](implicit elementReader: Reader[A]): Reader[List[A]] =
    new Reader[List[A]] {
      override type DriverType = java.util.List[elementReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.List[elementReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.List[elementReader.DriverType],
        dataType: DataType
      ): List[A] = {
        println("Calling listReader.convertDriverToScala")
        val elemDataType = dataType.asInstanceOf[ListType].getElementType
        driverValue.asScala.map(elementReader.convertDriverToScala(_, elemDataType)).toList
      }

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): java.util.List[elementReader.DriverType] =
        structure.getList(fieldName.get, elementReader.driverClass)
    }

  implicit def setReader[A](implicit elementReader: Reader[A]): Reader[Set[A]] =
    new Reader[Set[A]] {
      override type DriverType = java.util.Set[elementReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.Set[elementReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Set[elementReader.DriverType],
        dataType: DataType
      ): Set[A] = {
        val elemType = dataType.asInstanceOf[SetType].getElementType
        driverValue.asScala.map(elementReader.convertDriverToScala(_, elemType)).toSet
      }

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        structure.getSet(fieldName.get, elementReader.driverClass)
    }

  implicit def mapReader[K, V](implicit
    keyReader: Reader[K],
    valueReader: Reader[V]
  ): Reader[Map[K, V]] =
    new Reader[Map[K, V]] {
      override type DriverType = java.util.Map[keyReader.DriverType, valueReader.DriverType]

      override def driverClass: Class[DriverType] = classOf[java.util.Map[keyReader.DriverType, valueReader.DriverType]]

      override def convertDriverToScala(
        driverValue: java.util.Map[keyReader.DriverType, valueReader.DriverType],
        dataType: DataType
      ): Map[K, V] = {
        val keyType   = dataType.asInstanceOf[MapType].getKeyType
        val valueType = dataType.asInstanceOf[MapType].getValueType
        driverValue.asScala.map { case (key, value) =>
          keyReader.convertDriverToScala(key, keyType) -> valueReader.convertDriverToScala(value, valueType)
        }.toMap
      }

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        structure.getMap(fieldName.get, keyReader.driverClass, valueReader.driverClass)
    }

  implicit def optionReader[A](implicit elementReader: Reader[A]): Reader[Option[A]] =
    new Reader[Option[A]] {
      override type DriverType = elementReader.DriverType

      override def driverClass: Class[DriverType] = elementReader.driverClass

      override def convertDriverToScala(driverValue: DriverType, dataType: DataType): Option[A] =
        Option(driverValue).map(elementReader.convertDriverToScala(_, dataType))

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        structure.get(fieldName.get, elementReader.driverClass)
    }

  private def identityDatatype[A](value: A, dataType: DataType): A = {
    val _ = dataType
    value
  }
}

trait UdtReaderMagnoliaDerivation {
  type Typeclass[T] = Reader[T]

  def join[T](ctx: CaseClass[Reader, T]): Reader.WithDriver[T, UdtValue] =
    new Typeclass[T] {
      override type DriverType = UdtValue

      override def driverClass: Class[DriverType] = classOf[UdtValue]

      override def convertDriverToScala(driverValue: DriverType, dataType: DataType): T =
        ctx.construct { param =>
          val fieldName   = param.label
          val fieldReader = param.typeclass
          val output      = fieldReader.read(driverValue, Option(fieldName))
          output
        }

      override def readFromDriver[Structure <: CassandraStructure](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        fieldName match {
          case Some(field) =>
            structure.getUdtValue(field)

          case None =>
            throw new RuntimeException("Cannot read a UDT Value from the root (Row), Please use a RowReader instead")
        }
    }

  implicit def deriveUdtValue[T]: Reader.WithDriver[T, UdtValue] = macro Magnolia.gen[T]
}
