package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

/**
 * A [[UdtValueEncoder]] encodes a Scala type [[A]] as a component of a
 * [[UdtValue]]
 *
 * @tparam A
 */
trait UdtValueEncoder[A] {
  def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue
  def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue
}
object UdtValueEncoder extends UdtValueEncoderMagnoliaDerivation {

  /**
   * A [[UdtValueEncoder.Object]] that encodes a Scala type [[A]] as an entire
   * [[UdtValue]]
   * @tparam A
   */
  trait Object[A] extends UdtValueEncoder[A] {
    def encode(structure: UdtValue, value: A): UdtValue
  }

  def apply[A](implicit encoder: UdtValueEncoder.Object[A]): UdtValueEncoder.Object[A] = encoder

  def custom[A](f: (UdtValue, A) => UdtValue): UdtValueEncoder.Object[A] = new UdtValueEncoder.Object[A] {
    override def encode(structure: UdtValue, value: A): UdtValue = f(structure, value)

    override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue =
      f(structure.getUdtValue(fieldName), value)

    override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue =
      f(structure.getUdtValue(index), value)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveEncoder[A]): UdtValueEncoder[A] =
    new UdtValueEncoder[A] {
      override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue = {
        val driverType = structure.getType(fieldName)
        val driver     = prim.scala2Driver(value, driverType)
        structure.set(fieldName, driver, prim.driverClass)
      }

      override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue = {
        val driverType = structure.getType(index)
        val driver     = prim.scala2Driver(value, driverType)
        structure.set(index, driver, prim.driverClass)
      }
    }
}

trait UdtValueEncoderMagnoliaDerivation {
  type Typeclass[T] = UdtValueEncoder[T]

  def join[T](ctx: CaseClass[UdtValueEncoder, T]): UdtValueEncoder.Object[T] =
    new UdtValueEncoder.Object[T] {
      override def encode(structure: UdtValue, value: T): UdtValue =
        ctx.parameters.foldLeft(structure) { case (acc, p) =>
          val fieldName  = p.label
          val fieldValue = p.dereference(value)
          val encoder    = p.typeclass
          encoder.encodeByFieldName(acc, fieldName, fieldValue)
        }

      override def encodeByFieldName(structure: UdtValue, fieldName: String, value: T): UdtValue =
        encode(structure.getUdtValue(fieldName), value)

      override def encodeByIndex(structure: UdtValue, index: Int, value: T): UdtValue =
        encode(structure.getUdtValue(index), value)
    }

  implicit def derive[T]: UdtValueEncoder.Object[T] = macro Magnolia.gen[T]
}
