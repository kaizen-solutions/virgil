package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

/**
 * A [[UdtValueEncoder]] encodes a Scala type [[A]] as a component of a
 * [[UdtValue]]
 *
 * @tparam A
 */
trait UdtValueEncoder[-A] {
  def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue
  def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue
}
object UdtValueEncoder extends UdtValueEncoderMagnoliaDerivation {

  /**
   * A [[UdtValueEncoder.Object]] that encodes a Scala type [[A]] as an entire
   * [[UdtValue]]. This is really contravariant in A but Magnolia's automatic
   * derivation process is disrupted
   * @tparam A
   */
  trait Object[A] extends UdtValueEncoder[A] { self =>
    def encode(structure: UdtValue, value: A): UdtValue

    override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue =
      encode(structure.getUdtValue(fieldName), value)

    override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue =
      encode(structure.getUdtValue(index), value)

    def contramap[B](f: B => A): UdtValueEncoder.Object[B] = new UdtValueEncoder.Object[B] {
      override def encode(structure: UdtValue, value: B): UdtValue = self.encode(structure, f(value))
    }

    // Safe to use a subtype because a subtype contains more information, therefore we can discard the extra information
    def narrow[B <: A]: UdtValueEncoder.Object[B] = contramap(identity)

    def zip[B](that: UdtValueEncoder.Object[B]): UdtValueEncoder.Object[(A, B)] =
      new UdtValueEncoder.Object[(A, B)] {
        def encode(structure: UdtValue, value: (A, B)): UdtValue = {
          val (a, b)   = value
          val resultA  = self.encode(structure, a)
          val resultAB = that.encode(resultA, b)
          resultAB
        }
      }
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
    }

  implicit def derive[T]: UdtValueEncoder.Object[T] = macro Magnolia.gen[T]
}
