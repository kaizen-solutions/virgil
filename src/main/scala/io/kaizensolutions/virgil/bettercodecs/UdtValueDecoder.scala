package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

trait UdtValueDecoder[A] {
  def decodeByFieldName(structure: UdtValue, fieldName: String): A
  def decodeByIndex(structure: UdtValue, index: Int): A
}
object UdtValueDecoder extends UdtValueDecoderMagnoliaDerivation {
  trait Object[A] extends UdtValueDecoder[A] {
    def decode(structure: UdtValue): A
  }

  def apply[A](implicit ev: UdtValueDecoder.Object[A]): UdtValueDecoder.Object[A] = ev

  def custom[A](f: UdtValue => A): UdtValueDecoder.Object[A] = new UdtValueDecoder.Object[A] {
    override def decode(structure: UdtValue): A = f(structure)

    override def decodeByFieldName(structure: UdtValue, fieldName: String): A =
      decode(structure.getUdtValue(fieldName))

    override def decodeByIndex(structure: UdtValue, index: Int): A =
      decode(structure.getUdtValue(index))
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveDecoder[A]): UdtValueDecoder[A] =
    new UdtValueDecoder[A] {
      override def decodeByFieldName(structure: UdtValue, fieldName: String): A = {
        val driver     = structure.get(fieldName, prim.driverClass)
        val driverType = structure.getType(fieldName)
        prim.driver2Scala(driver, driverType)
      }

      override def decodeByIndex(structure: UdtValue, index: Int): A = {
        val driver     = structure.get(index, prim.driverClass)
        val driverType = structure.getType(index)
        prim.driver2Scala(driver, driverType)
      }
    }
}
trait UdtValueDecoderMagnoliaDerivation {
  type Typeclass[T] = UdtValueDecoder[T]

  def join[T](ctx: CaseClass[UdtValueDecoder, T]): UdtValueDecoder.Object[T] =
    new UdtValueDecoder.Object[T] {
      override def decode(structure: UdtValue): T =
        ctx.construct { param =>
          val fieldName = param.label
          val decoder   = param.typeclass
          decoder.decodeByFieldName(structure, fieldName)
        }

      override def decodeByFieldName(structure: UdtValue, fieldName: String): T =
        decode(structure.getUdtValue(fieldName))

      override def decodeByIndex(structure: UdtValue, index: Int): T =
        decode(structure.getUdtValue(index))
    }

  implicit def derive[T]: UdtValueDecoder.Object[T] = macro Magnolia.gen[T]
}
