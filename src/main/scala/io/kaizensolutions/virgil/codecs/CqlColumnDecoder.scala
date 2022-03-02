package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{CqlDuration, GettableByName, TupleValue, UdtValue}
import zio.Chunk
import zio.schema.Schema.Primitive
import zio.schema.{Schema, StandardType}

import java.nio.ByteBuffer
import java.time._
import java.util
import java.util.UUID
import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

/**
 * Reader for Cassandra data types.
 * @see
 *   https://docs.datastax.com/en/developer/java-driver/4.13/manual/core/#cql-to-java-type-mapping
 * @tparam ScalaType
 */
@implicitNotFound(
  "No CqlColumnDecoder found for ${ScalaType}, please use CqlColumnDecoder.fromSchema[${ScalaType}] to derive one and make sure a zio.schema.Schema[${ScalaType}] is defined and marked implicit"
)
sealed trait CqlColumnDecoder[ScalaType] { self =>
  type DriverType

  def driverClass: Class[DriverType]

  def convertDriverToScala(driverValue: DriverType): ScalaType

  def readFieldFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType

  def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): DriverType

  final def decodeFieldByName[Structure <: GettableByName](structure: Structure, fieldName: String): ScalaType =
    convertDriverToScala(driverValue = readFieldFromDriver(structure, fieldName))

  final def decodeFieldByIndex[Structure <: GettableByName](structure: Structure, fieldIndex: Int): ScalaType =
    convertDriverToScala(driverValue = readIndexFromDriver(structure, fieldIndex))

  final def map[ScalaType2](f: ScalaType => ScalaType2): CqlColumnDecoder.WithDriver[ScalaType2, DriverType] =
    new CqlColumnDecoder[ScalaType2] {
      override type DriverType = self.DriverType

      override def driverClass: Class[DriverType] = self.driverClass

      override def convertDriverToScala(driverValue: DriverType): ScalaType2 =
        f(self.convertDriverToScala(driverValue))

      def readFieldFromDriver[Structure <: GettableByName](structure: Structure, fieldName: String): DriverType =
        self.readFieldFromDriver(structure, fieldName)

      override def readIndexFromDriver[Structure <: GettableByName](
        structure: Structure,
        index: Int
      ): CqlColumnDecoder.this.DriverType =
        self.readIndexFromDriver(structure, index)
    }

  final def orDie[L, R](implicit ev: ScalaType =:= Either[L, R]): CqlColumnDecoder.WithDriver[R, DriverType] =
    self.map { in =>
      ev(in) match {
        case Left(error)  => throw new RuntimeException(s"Failed to map input to ${driverClass.getName}: $error")
        case Right(value) => value
      }
    }
}
object CqlColumnDecoder {
  type WithDriver[Scala, Driver] = CqlColumnDecoder[Scala] { type DriverType = Driver }

  def apply[A](implicit ev: CqlColumnDecoder[A]): CqlColumnDecoder[A] = ev

  def make[Scala, Driver](
    driverClassInfo: Class[Driver]
  )(convert: Driver => Scala)(
    fieldFn: (GettableByName, String) => Driver
  )(indexFn: (GettableByName, Int) => Driver): CqlColumnDecoder.WithDriver[Scala, Driver] =
    new CqlColumnDecoder[Scala] {
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

  def fromUdtValue[A](f: UdtValue => A): CqlColumnDecoder.WithDriver[A, UdtValue] = new CqlColumnDecoder[A] {
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

  def fromRow[A](f: Row => A): CqlColumnDecoder.WithDriver[A, Row] = new CqlColumnDecoder[A] {
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

  implicit val unitColumnDecoder: CqlColumnDecoder.WithDriver[Unit, Unit] =
    make(classOf[Unit])(identity)((_, _) => ())((_, _) => ())

  implicit val chunkByteDecoder: CqlColumnDecoder.WithDriver[Chunk[Byte], ByteBuffer] =
    make(classOf[ByteBuffer])(Chunk.fromByteBuffer)((structure, columnName) => structure.getByteBuffer(columnName))(
      (structure, index) => structure.getByteBuffer(index)
    )

  implicit val javaBigDecimalColumnDecoder: CqlColumnDecoder.WithDriver[java.math.BigDecimal, java.math.BigDecimal] =
    make(classOf[java.math.BigDecimal])(identity)((structure, columnName) => structure.getBigDecimal(columnName))(
      (structure, index) => structure.getBigDecimal(index)
    )

  implicit val bigDecimalColumnDecoder: CqlColumnDecoder.WithDriver[BigDecimal, java.math.BigDecimal] =
    javaBigDecimalColumnDecoder.map(BigDecimal.javaBigDecimal2bigDecimal)

  implicit val javaBigIntColumnDecoder: CqlColumnDecoder.WithDriver[java.math.BigInteger, java.math.BigInteger] =
    make(classOf[java.math.BigInteger])(identity)((structure, columnName) => structure.getBigInteger(columnName))(
      (structure, index) => structure.getBigInteger(index)
    )

  implicit val bigIntColumnDecoder: CqlColumnDecoder.WithDriver[BigInt, java.math.BigInteger] =
    javaBigIntColumnDecoder.map(BigInt.javaBigInteger2bigInt)

  implicit val booleanColumnDecoder: CqlColumnDecoder.WithDriver[Boolean, java.lang.Boolean] =
    make(classOf[java.lang.Boolean])(Boolean.unbox)((structure, columnName) => structure.getBoolean(columnName))(
      (structure, index) => structure.getBoolean(index)
    )

  implicit val byteBufferColumnDecoder: CqlColumnDecoder.WithDriver[java.nio.ByteBuffer, java.nio.ByteBuffer] =
    make(classOf[java.nio.ByteBuffer])(identity)((structure, columnName) => structure.getByteBuffer(columnName))(
      (structure, index) => structure.getByteBuffer(index)
    )

  implicit val byteColumnDecoder: CqlColumnDecoder.WithDriver[Byte, java.lang.Byte] =
    make(classOf[java.lang.Byte])(Byte.unbox)((structure, columnName) => structure.getByte(columnName))(
      (structure, index) => structure.getByte(index)
    )

  implicit val cqlTupleValueColumnDecoder: CqlColumnDecoder.WithDriver[TupleValue, TupleValue] =
    make(classOf[TupleValue])(identity)((structure, columnName) => structure.getTupleValue(columnName))(
      (structure, index) => structure.getTupleValue(index)
    )

  implicit val doubleColumnDecoder: CqlColumnDecoder.WithDriver[Double, java.lang.Double] =
    make(classOf[java.lang.Double])(Double.unbox)((structure, columnName) => structure.getDouble(columnName))(
      (structure, index) => structure.getDouble(index)
    )

  implicit val cqlDurationColumnDecoder: CqlColumnDecoder.WithDriver[CqlDuration, CqlDuration] =
    make(classOf[CqlDuration])(identity)((structure, columnName) => structure.getCqlDuration(columnName))(
      (structure, index) => structure.getCqlDuration(index)
    )

  implicit val floatColumnDecoder: CqlColumnDecoder.WithDriver[Float, java.lang.Float] =
    make(classOf[java.lang.Float])(Float.unbox)((structure, columnName) => structure.getFloat(columnName))(
      (structure, index) => structure.getFloat(index)
    )

  implicit val inetAddressColumnDecoder: CqlColumnDecoder.WithDriver[java.net.InetAddress, java.net.InetAddress] =
    make(classOf[java.net.InetAddress])(identity)((structure, columnName) => structure.getInetAddress(columnName))(
      (structure, index) => structure.getInetAddress(index)
    )

  implicit val instantColumnDecoder: CqlColumnDecoder.WithDriver[java.time.Instant, java.time.Instant] =
    make(classOf[java.time.Instant])(identity)((structure, columnName) => structure.getInstant(columnName))(
      (structure, index) => structure.getInstant(index)
    )

  implicit val localDateTimeDecoderUsingUTC: CqlColumnDecoder.WithDriver[LocalDateTime, Instant] =
    CqlColumnDecoder.instantColumnDecoder.map(i => LocalDateTime.ofInstant(i, ZoneOffset.UTC))

  implicit val zonedDateTimeDecoderUsingUTC: CqlColumnDecoder.WithDriver[ZonedDateTime, Instant] =
    CqlColumnDecoder.instantColumnDecoder.map(i => ZonedDateTime.ofInstant(i, ZoneOffset.UTC))

  implicit val offsetDateTimeDecoderUsingUTC: CqlColumnDecoder.WithDriver[OffsetDateTime, Instant] =
    CqlColumnDecoder.instantColumnDecoder.map(i => OffsetDateTime.ofInstant(i, ZoneOffset.UTC))

  implicit val intColumnDecoder: CqlColumnDecoder.WithDriver[Int, Integer] =
    make(classOf[java.lang.Integer])(Int.unbox)((structure, columnName) => structure.getInt(columnName))(
      (structure, index) => structure.getInt(index)
    )

  implicit val localDateColumnDecoder: CqlColumnDecoder.WithDriver[java.time.LocalDate, java.time.LocalDate] =
    make(classOf[java.time.LocalDate])(identity)((structure, columnName) => structure.getLocalDate(columnName))(
      (structure, index) => structure.getLocalDate(index)
    )

  implicit val localTimeColumnDecoder: CqlColumnDecoder.WithDriver[java.time.LocalTime, java.time.LocalTime] =
    make(classOf[java.time.LocalTime])(identity)((structure, columnName) => structure.getLocalTime(columnName))(
      (structure, index) => structure.getLocalTime(index)
    )

  implicit val offsetTimeColumnDecoderUsingUTC: CqlColumnDecoder.WithDriver[java.time.OffsetTime, java.time.LocalTime] =
    localTimeColumnDecoder.map(l => java.time.OffsetTime.of(l, ZoneOffset.UTC))

  implicit val longColumnDecoder: CqlColumnDecoder.WithDriver[Long, java.lang.Long] =
    make(classOf[java.lang.Long])(Long.unbox)((structure, columnName) => structure.getLong(columnName))(
      (structure, index) => structure.getLong(index)
    )

  implicit val shortColumnDecoder: CqlColumnDecoder.WithDriver[Short, java.lang.Short] =
    make(classOf[java.lang.Short])(s => Short.unbox(s))((structure, columnName) => structure.getShort(columnName))(
      (structure, index) => structure.getShort(index)
    )

  implicit val stringColumnDecoder: CqlColumnDecoder.WithDriver[String, java.lang.String] =
    make(classOf[java.lang.String])(identity)((structure, columnName) => structure.getString(columnName))(
      (structure, index) => structure.getString(index)
    )

  implicit val uuidColumnDecoder: CqlColumnDecoder.WithDriver[java.util.UUID, java.util.UUID] =
    make(classOf[java.util.UUID])(identity)((structure, columnName) => structure.getUuid(columnName))(
      (structure, index) => structure.getUuid(index)
    )

  implicit val rowColumnDecoder: CqlColumnDecoder.WithDriver[Row, Row] =
    make(classOf[Row])(identity)((structure, _) => structure.asInstanceOf[Row])((structure, _) =>
      structure.asInstanceOf[Row]
    )

  implicit val udtValueDecoder: CqlColumnDecoder.WithDriver[UdtValue, UdtValue] =
    make(classOf[UdtValue])(identity)((structure, columnName) => structure.getUdtValue(columnName))(
      (structure, index) => structure.getUdtValue(index)
    )

  implicit def listColumnDecoder[A](implicit elementColumnDecoder: CqlColumnDecoder[A]): CqlColumnDecoder[List[A]] =
    new CqlColumnDecoder[List[A]] {
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

  implicit def setColumnDecoder[A](implicit elementColumnDecoder: CqlColumnDecoder[A]): CqlColumnDecoder[Set[A]] =
    new CqlColumnDecoder[Set[A]] {
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
    keyColumnDecoder: CqlColumnDecoder[K],
    valueColumnDecoder: CqlColumnDecoder[V]
  ): CqlColumnDecoder[Map[K, V]] =
    new CqlColumnDecoder[Map[K, V]] {
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

  implicit def optionColumnDecoder[A](implicit elementColumnDecoder: CqlColumnDecoder[A]): CqlColumnDecoder[Option[A]] =
    new CqlColumnDecoder[Option[A]] {
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

  implicit def fromSchema[A](implicit schema: Schema[A]): CqlColumnDecoder[A] =
    schema match {
      case Primitive(standardType, _) =>
        primitiveDecoder(standardType)

      case `cqlDurationSchema` =>
        CqlColumnDecoder[CqlDuration].map(_.asInstanceOf[A])

      case record: Schema.Record[a] =>
        val fields = decodeFieldsOfRecord(record)

        new CqlColumnDecoder[A] {
          override type DriverType = UdtValue

          override def driverClass: Class[DriverType] = classOf[UdtValue]

          override def convertDriverToScala(driverValue: UdtValue): A = {
            val raw = fields.map { case (name, decoder) =>
              decoder.decodeFieldByName(driverValue, name)
            }
            record
              .rawConstruct(raw)
              .getOrElse(
                throw new IllegalArgumentException(
                  s"Failed to construct $record from UDT Value with the following components ${driverValue.getFormattedContents}"
                )
              )
              .asInstanceOf[A]
          }

          override def readFieldFromDriver[Structure <: GettableByName](
            structure: Structure,
            fieldName: String
          ): DriverType = structure.getUdtValue(fieldName)

          override def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): DriverType =
            structure.getUdtValue(index)
        }
      case Schema.Sequence(elemSchema, _, _, _) =>
        val elementDecoder = fromSchema(elemSchema)
        listColumnDecoder(elementDecoder).map(_.asInstanceOf[A])

      case Schema.MapSchema(keySchema, valueSchema, _) =>
        val keyDecoder   = fromSchema(keySchema)
        val valueDecoder = fromSchema(valueSchema)
        mapColumnDecoder(keyDecoder, valueDecoder).map(_.asInstanceOf[A])

      case Schema.SetSchema(elemSchema, _) =>
        val elementDecoder = fromSchema(elemSchema)
        setColumnDecoder(elementDecoder).map(_.asInstanceOf[A])

      case Schema.Lazy(schemaFn) => fromSchema(schemaFn())

      case t: Schema.Transform[a, b] =>
        val schemaForA: Schema[a] = t.codec
        fromSchema(schemaForA)
          .map(t.f)
          .orDie

      case o: Schema.Optional[a] =>
        val decoder = fromSchema(o.codec)
        new CqlColumnDecoder[Option[a]] {
          override type DriverType = decoder.DriverType

          override def driverClass: Class[DriverType] = decoder.driverClass

          override def convertDriverToScala(driverValue: DriverType): Option[a] =
            Option(driverValue).map(decoder.convertDriverToScala)

          override def readFieldFromDriver[Structure <: GettableByName](
            structure: Structure,
            fieldName: String
          ): DriverType = decoder.readFieldFromDriver(structure, fieldName)

          override def readIndexFromDriver[Structure <: GettableByName](structure: Structure, index: Int): DriverType =
            decoder.readIndexFromDriver(structure, index)
        }

      case t: Schema.Tuple[a, b] =>
        throw new RuntimeException(s"Tuple is not supported: $t")

      case Schema.Fail(message, _) =>
        throw new RuntimeException(s"Schema failed to decode: $message")

      case enum: Schema.Enum[_] =>
        throw new RuntimeException(s"Enumeration failed to decode: $enum")

      case e @ Schema.EitherSchema(_, _, _) =>
        throw new RuntimeException(s"Either Schema failed to decode: $e")

      case Schema.Meta(ast, _) => throw new RuntimeException(s"Meta is not supported: $ast")

      case s =>
        throw new RuntimeException(s"Unsupported schema: $s")
    }

  private def decodeFieldsOfRecord[A](record: Schema.Record[A]) =
    record.structure.map { field =>
      val fieldName = field.label
      val decoder   = fromSchema(field.schema)
      (fieldName, decoder)
    }

  private def primitiveDecoder[A](typ: StandardType[A]): CqlColumnDecoder[A] =
    typ match {
      case StandardType.UnitType              => CqlColumnDecoder[Unit]
      case StandardType.StringType            => CqlColumnDecoder[String]
      case StandardType.BoolType              => CqlColumnDecoder[Boolean]
      case StandardType.ShortType             => CqlColumnDecoder[Short]
      case StandardType.IntType               => CqlColumnDecoder[Int]
      case StandardType.LongType              => CqlColumnDecoder[Long]
      case StandardType.FloatType             => CqlColumnDecoder[Float]
      case StandardType.DoubleType            => CqlColumnDecoder[Double]
      case StandardType.BinaryType            => CqlColumnDecoder[Chunk[Byte]]
      case StandardType.UUIDType              => CqlColumnDecoder[UUID]
      case StandardType.BigDecimalType        => CqlColumnDecoder[java.math.BigDecimal]
      case StandardType.BigIntegerType        => CqlColumnDecoder[java.math.BigInteger]
      case StandardType.InstantType(_)        => CqlColumnDecoder[Instant]
      case StandardType.LocalDateType(_)      => CqlColumnDecoder[LocalDate]
      case StandardType.LocalTimeType(_)      => CqlColumnDecoder[LocalTime]
      case StandardType.LocalDateTimeType(_)  => CqlColumnDecoder[LocalDateTime]
      case StandardType.OffsetTimeType(_)     => CqlColumnDecoder[OffsetTime]
      case StandardType.OffsetDateTimeType(_) => CqlColumnDecoder[OffsetDateTime]
      case StandardType.ZonedDateTimeType(_)  => CqlColumnDecoder[ZonedDateTime]
      case t @ StandardType.Duration(_)       => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.ZoneIdType        => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.ZoneOffsetType    => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.CharType          => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.DayOfWeekType     => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.MonthType         => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.MonthDayType      => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.PeriodType        => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.YearType          => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
      case t @ StandardType.YearMonthType     => throw new RuntimeException(s"$t not supported for CqlColumnDecoder")
    }
}
