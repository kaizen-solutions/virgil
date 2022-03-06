package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.data.UdtValue
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1._

/**
 * A [[CqlUdtValueEncoder]] encodes a Scala type [[A]] as a component of a
 * [[UdtValue]]
 *
 * @tparam A
 */
trait CqlUdtValueEncoder[-A] {
  def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue
  def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue
}
object CqlUdtValueEncoder extends UdtValueEncoderMagnoliaDerivation {

  /**
   * A [[CqlUdtValueEncoder.Object]] that encodes a Scala type [[A]] as an
   * entire [[UdtValue]]. This is really contravariant in A but Magnolia's
   * automatic derivation process is disrupted
   *
   * @tparam A
   */
  trait Object[A] extends CqlUdtValueEncoder[A] { self =>
    def encode(structure: UdtValue, value: A): UdtValue

    override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue =
      encode(structure.getUdtValue(fieldName), value)

    override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue =
      encode(structure.getUdtValue(index), value)

    def contramap[B](f: B => A): CqlUdtValueEncoder.Object[B] = new CqlUdtValueEncoder.Object[B] {
      override def encode(structure: UdtValue, value: B): UdtValue = self.encode(structure, f(value))
    }

    // Safe to use a subtype because a subtype contains more information, therefore we can discard the extra information
    // prior to writing the data
    def narrow[B <: A]: CqlUdtValueEncoder.Object[B] = contramap(identity)

    def zip[B](that: CqlUdtValueEncoder.Object[B]): CqlUdtValueEncoder.Object[(A, B)] =
      new CqlUdtValueEncoder.Object[(A, B)] {
        def encode(structure: UdtValue, value: (A, B)): UdtValue = {
          val (a, b)   = value
          val resultA  = self.encode(structure, a)
          val resultAB = that.encode(resultA, b)
          resultAB
        }
      }
  }

  def apply[A](implicit encoder: CqlUdtValueEncoder.Object[A]): CqlUdtValueEncoder.Object[A] = encoder

  def custom[A](f: (UdtValue, A) => UdtValue): CqlUdtValueEncoder.Object[A] = new CqlUdtValueEncoder.Object[A] {
    override def encode(structure: UdtValue, value: A): UdtValue = f(structure, value)

    override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue =
      f(structure.getUdtValue(fieldName), value)

    override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue =
      f(structure.getUdtValue(index), value)
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveEncoder[A]): CqlUdtValueEncoder[A] =
    new CqlUdtValueEncoder[A] {
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
  type Typeclass[T] = CqlUdtValueEncoder[T]

  def join[T](ctx: CaseClass[CqlUdtValueEncoder, T]): CqlUdtValueEncoder.Object[T] =
    new CqlUdtValueEncoder.Object[T] {
      override def encode(structure: UdtValue, value: T): UdtValue =
        ctx.parameters.foldLeft(structure) { case (acc, p) =>
          val fieldName  = CqlColumn.extractFieldName(p.annotations).getOrElse(p.label)
          val fieldValue = p.dereference(value)
          val encoder    = p.typeclass
          encoder.encodeByFieldName(acc, fieldName, fieldValue)
        }
    }

  implicit def derive[T]: CqlUdtValueEncoder.Object[T] = macro Magnolia.gen[T]
}
