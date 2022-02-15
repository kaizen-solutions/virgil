package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`._
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.{SettableByName, UdtValue}
import magnolia1._

import scala.jdk.CollectionConverters._

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
trait FieldWriterMagnoliaDerivation {
  type Typeclass[T] = Writer[T]

  def join[T](ctx: CaseClass[Writer, T]): Writer.WithDriver[T, Row] = new Writer[T] {
    override type DriverType = Row

    override def driverClass: Class[DriverType] = classOf[DriverType]

    override def convertScalaToDriver(scalaValue: T, dataType: DataType): Row = null

    override def write[Structure <: SettableByName[Structure]](
      key: String,
      value: T,
      structure: Structure
    ): Structure = ctx.parameters.foldLeft(structure) { case (s, p) =>
      p.typeclass.write(p.label, p.dereference(value), s)
    }
  }

  def deriveRow[T]: Writer[T] = macro Magnolia.gen[T]
}

trait UdtWriterMagnoliaDerivation {
  type Typeclass[T] = Writer[T]

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
