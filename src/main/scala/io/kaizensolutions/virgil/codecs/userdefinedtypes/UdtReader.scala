package io.kaizensolutions.virgil.codecs.userdefinedtypes

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.codecs.{CassandraTypeMapper, FieldName, Reader}
import magnolia1._

trait UdtReader[ScalaType] { self =>
  def read(name: FieldName, cassandra: UdtValue): ScalaType

  def map[ScalaType2](f: ScalaType => ScalaType2): UdtReader[ScalaType2] = (name: FieldName, cassandra: UdtValue) =>
    f(self.read(name, cassandra))

  def zipWith[ScalaType2, ResultScalaType](that: UdtReader[ScalaType2])(
    f: (ScalaType, ScalaType2) => ResultScalaType
  ): UdtReader[ResultScalaType] =
    (name: FieldName, cassandra: UdtValue) => f(self.read(name, cassandra), that.read(name, cassandra))

  def zip[ScalaType2](that: UdtReader[ScalaType2]): UdtReader[(ScalaType, ScalaType2)] =
    zipWith(that)((_, _))
}
object UdtReader extends MagnoliaUdtReaderSupport {
  // Indicates a UdtReader that is a case class and not a small part of a UdtReader
  // This can only be created by Magnolia's machinery
  trait CaseClass[ScalaType] extends UdtReader[ScalaType]

  def apply[A](implicit ev: UdtReader.CaseClass[A]): UdtReader[A] = ev

  /**
   * Derives a Reader from a UdtReader, you would use this when you have a UDT
   * value in your Row and you want to read it as a case class
   *
   * @param ev
   *   is the capability to read the UdtValue as a ScalaType
   * @tparam ScalaType
   *   is the type you want to view the UdtValue as
   * @return
   */
  def deriveReader[ScalaType](implicit ev: UdtReader.CaseClass[ScalaType]): Reader[ScalaType] = {
    (columnName: String, row: Row) =>
      val udtValue = row.getUdtValue(columnName)
      ev.read(FieldName.Unused, udtValue)
  }

  def make[A](f: UdtValue => A): UdtReader[A] =
    (name: FieldName, cassandra: UdtValue) =>
      name match {
        case FieldName.Unused => f(cassandra)
        case FieldName.Labelled(value) =>
          throw new IllegalStateException(
            s"UdtReader.make failure: Expected an unused Field Name for ${cassandra.getType.describe(true)} but was instead passed $value"
          )
      }

  def makeWithFieldName[A](f: (String, UdtValue) => A): UdtReader[A] =
    (name: FieldName, cassandra: UdtValue) =>
      name match {
        case FieldName.Unused =>
          throw new IllegalStateException(
            s"UdtReader.makeWithFieldName failure: Expected a labelled Field Name for ${cassandra.getType
              .describe(true)} but was instead passed FieldName.Unused"
          )

        case FieldName.Labelled(name) => f(name, cassandra)

      }

  /**
   * fromUdtValue exposes a low level API to read from a Cassandra UdtValue in
   * case you don't want to use derivation. You can use this in addition with
   * zipWith to compose small UdtReaders together to form a larger UdtReader.
   * @param f
   * @tparam A
   * @return
   */
  def fromUdtValue[A](f: UdtValue => A): UdtReader.CaseClass[A] =
    (_: FieldName, cassandra: UdtValue) => f(cassandra)

  implicit def deriveUdtReaderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): UdtReader[A] =
    makeWithFieldName { (name, udtValue) =>
      val cassandra     = udtValue.get(name, ev.classType)
      val cassandraType = udtValue.getType(name)
      ev.fromCassandra(cassandra, cassandraType)
    }
}

trait MagnoliaUdtReaderSupport {
  type Typeclass[T] = UdtReader[T]

  def join[T](ctx: CaseClass[UdtReader, T]): UdtReader.CaseClass[T] = new UdtReader.CaseClass[T] {
    override def read(fieldName: FieldName, cassandra: UdtValue): T =
      fieldName match {
        case FieldName.Unused =>
          ctx.construct { param =>
            val name = param.label
            param.typeclass.read(FieldName.Labelled(name), cassandra)
          }

        case FieldName.Labelled(name) =>
          // We're looking at a case class within a case class - so we need to adjust the UdtValue accordingly
          val nestedUdtValue = cassandra.getUdtValue(name)
          ctx.construct { param =>
            val name = param.label
            param.typeclass.read(FieldName.Labelled(name), nestedUdtValue)
          }
      }
  }

  // Fully automatic derivation enabling usage of deriveReader
  implicit def gen[T]: UdtReader.CaseClass[T] = macro Magnolia.gen[T]
}
