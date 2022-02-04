package io.kaizensolutions.virgil.codecs.userdefinedtypes

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.codecs.{CassandraTypeMapper, FieldName, Writer}
import magnolia1._

import scala.language.experimental.macros

trait UdtWriter[ScalaType] { self =>
  def write(name: FieldName, value: ScalaType, udt: UdtValue): UdtValue

  def contramap[ScalaType2](f: ScalaType2 => ScalaType): UdtWriter[ScalaType2] = (name, value, udt) =>
    self.write(name, f(value), udt)
}
object UdtWriter extends MagnoliaUdtWriterSupport {
  // Indicates a UdtWriter that is a case class and not a small part of a UdtWriter
  // This can only be created by Magnolia's machinery
  trait CaseClass[ScalaType] extends UdtWriter[ScalaType]

  def apply[A](implicit ev: UdtWriter.CaseClass[A]): UdtWriter[A] = ev

  def deriveWriter[ScalaType](implicit ev: UdtWriter.CaseClass[ScalaType]): Writer[ScalaType] = {
    (builder: BoundStatementBuilder, column: String, value: ScalaType) =>
      val userDefinedType = builder.getType(column).asInstanceOf[UserDefinedType]
      // Since this is a top level case class, it maps 1:1 with a UDT and we are building it here
      // you supply labelled fieldNames when constructing parts of the UdtWriter
      val udtValue = ev.write(FieldName.Unused, value, userDefinedType.newValue())
      builder.setUdtValue(column, udtValue)
  }

  def make[A](f: (A, UdtValue) => UdtValue): UdtWriter[A] = (name, value, udt) =>
    name match {
      case FieldName.Unused => f(value, udt)
      case FieldName.Labelled(value) =>
        throw new IllegalStateException(
          s"UdtWriter.make failure: Expected an unused Field Name for ${udt.getType.describe(true)} but was instead passed $value"
        )
    }

  def makeWithFieldName[A](f: (String, A, UdtValue) => UdtValue): UdtWriter[A] = (name, value, udt) =>
    name match {
      case FieldName.Unused =>
        throw new IllegalStateException(
          s"UdtWriter.makeWithFieldName failure: Expected a labelled Field Name for ${udt.getType.describe(true)} but was instead passed unused"
        )

      case FieldName.Labelled(name) => f(name, value, udt)
    }

  implicit def deriveUdtWriterFromCassandraTypeMapper[ScalaType](implicit
    ev: CassandraTypeMapper[ScalaType]
  ): UdtWriter[ScalaType] = makeWithFieldName[ScalaType] { (name, value, udt) =>
    val cassandraType = udt.getType(name)
    val cassandra     = ev.toCassandra(value, cassandraType)
    udt.set[ev.Cassandra](name, cassandra, ev.classType)
  }
}
trait MagnoliaUdtWriterSupport {
  type Typeclass[T] = UdtWriter[T]

  def join[T](ctx: CaseClass[UdtWriter, T]): UdtWriter.CaseClass[T] = new UdtWriter.CaseClass[T] {
    override def write(name: FieldName, value: T, udt: UdtValue): UdtValue =
      name match {
        case FieldName.Unused =>
          ctx.parameters.foldLeft(udt) { case (acc, param) =>
            val fieldName = param.label
            val cassandra = param.dereference(value)
            param.typeclass.write(FieldName.Labelled(fieldName), cassandra, acc)
          }

        case FieldName.Labelled(fieldName) =>
          // We're looking at a case class within a case class - so we need to adjust the UdtValue accordingly
          val initial = udt.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
          val serialized: UdtValue =
            ctx.parameters.foldLeft(initial) { case (acc, param) =>
              val fieldName = param.label
              val cassandra = param.dereference(value)
              param.typeclass.write(FieldName.Labelled(fieldName), cassandra, acc)
            }
          udt.setUdtValue(fieldName, serialized)
      }
  }

  // Fully automatic derivation enabling usage of deriveWriter
  implicit def gen[T]: UdtWriter.CaseClass[T] = macro Magnolia.gen[T]
}
