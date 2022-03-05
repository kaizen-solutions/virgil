package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.cql.Row
import magnolia1._

trait RowDecoder[A] {
  def decodeByFieldName(row: Row, fieldName: String): A
  def decodeByIndex(row: Row, index: Int): A
}
object RowDecoder extends RowDecoderMagnoliaDerivation {
  trait Object[A] extends RowDecoder[A] {
    def decode(row: Row): A

    // You cannot have nested Rows within Rows
    // You can have nested UdtValues within Rows which is taken care of (using fromCqlPrimitive)
    // Magnolia dispatches to fromCqlPrimitive which generates instances and prevents recursion on this typeclass from taking place
    def decodeByFieldName(row: Row, fieldName: String): A = decode(row)
    def decodeByIndex(row: Row, index: Int): A            = decode(row)
  }

  def apply[A](implicit ev: RowDecoder.Object[A]): RowDecoder.Object[A] = ev

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): RowDecoder[A] = new RowDecoder[A] {
    override def decodeByFieldName(row: Row, fieldName: String): A = {
      val driverType = row.getType(fieldName)
      val driver     = row.get(fieldName, prim.driverClass)
      prim.driver2Scala(driver, driverType)
    }

    override def decodeByIndex(row: Row, index: Int): A = {
      val driverType = row.getType(index)
      val driver     = row.get(index, prim.driverClass)
      prim.driver2Scala(driver, driverType)
    }
  }
}
trait RowDecoderMagnoliaDerivation {
  type Typeclass[T] = RowDecoder[T]

  def join[T](ctx: CaseClass[RowDecoder, T]): RowDecoder.Object[T] =
    new RowDecoder.Object[T] {
      override def decode(row: Row): T =
        ctx.construct { p =>
          val fieldName = p.label
          p.typeclass.decodeByFieldName(row, fieldName)
        }
    }

  implicit def derive[T]: RowDecoder.Object[T] = macro Magnolia.gen[T]
}
