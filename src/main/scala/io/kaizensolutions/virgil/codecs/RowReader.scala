package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.GettableByName
import magnolia1._

object RowReader {
  type Typeclass[T] = Reader[T]

  def join[T](ctx: CaseClass[Reader, T]): Reader.WithDriver[T, Row] =
    new Typeclass[T] {
      override type DriverType = Row

      override def driverClass: Class[DriverType] = classOf[DriverType]

      override def convertDriverToScala(driverValue: DriverType, dataType: DataType): T =
        ctx.construct { param =>
          val fieldName   = param.label
          val fieldReader = param.typeclass
          println(s"About to read $fieldName")
          val output = fieldReader.read(driverValue, Option(fieldName))
          println(s"Read $fieldName - output: $output")
          output
        }

      override def readFromDriver[Structure <: GettableByName](
        structure: Structure,
        fieldName: Option[String]
      ): DriverType =
        fieldName match {
          case Some(field) =>
            throw new RuntimeException(
              s"Cannot have nested Rows (you want a UDT instead), only top level - you requested ${field}"
            )

          case None =>
            structure.asInstanceOf[Row]
        }
    }

  def derive[T]: Reader.WithDriver[T, Row] = macro Magnolia.gen[T]
}
