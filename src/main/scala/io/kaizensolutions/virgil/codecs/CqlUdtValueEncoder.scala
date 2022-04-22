package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.data.UdtValue

/**
 * A [[CqlUdtValueEncoder]] encodes a Scala type `A` as a component of a
 * `UdtValue`
 *
 * @tparam A
 */
trait CqlUdtValueEncoder[-A] {
  def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue
  def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue
}
object CqlUdtValueEncoder extends UdtValueEncoderMagnoliaDerivation {

  /**
   * A [[CqlUdtValueEncoder.Object]] that encodes a Scala type `A` as an entire
   * `UdtValue`. This is really contravariant in A but Magnolia's automatic
   * derivation process is disrupted
   *
   * @tparam A
   */
  trait Object[A] extends CqlUdtValueEncoder[A] { self =>
    def encode(structure: UdtValue, value: A): UdtValue

    override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue = {
      val emptySubStructure  = structure.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
      val filledSubStructure = encode(emptySubStructure, value)
      structure.setUdtValue(fieldName, filledSubStructure)
    }

    override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue = {
      val emptySubStructure  = structure.getType(index).asInstanceOf[UserDefinedType].newValue()
      val filledSubStructure = encode(emptySubStructure, value)
      structure.setUdtValue(index, filledSubStructure)
    }

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
  }

  implicit def fromCqlPrimitive[A](implicit prim: CqlPrimitiveEncoder[A]): CqlUdtValueEncoder[A] =
    new CqlUdtValueEncoder[A] {
      override def encodeByFieldName(structure: UdtValue, fieldName: String, value: A): UdtValue =
        CqlPrimitiveEncoder.encodePrimitiveByFieldName(structure, fieldName, value)(prim)

      override def encodeByIndex(structure: UdtValue, index: Int, value: A): UdtValue =
        CqlPrimitiveEncoder.encodePrimitiveByIndex(structure, index, value)(prim)
    }
}
