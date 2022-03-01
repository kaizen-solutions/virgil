package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`.{ListType, UserDefinedType}
import com.datastax.oss.driver.api.core.data.{SettableByName, UdtValue}
import magnolia1._

import scala.jdk.CollectionConverters._

/**
 * case class Address(zipCode: String, city: String, street: String) case class
 * Person(id: Int, name: String, age: Int, address: Address)
 *
 * @tparam ScalaType
 */

trait CqlEncoder[ScalaType] {
  protected[virgil] def encodeByName[S <: SettableByName[S]](fieldName: String, value: ScalaType, structure: S): S = {
    val _ = fieldName
    encode(value, structure)
  }
  // Unused at the moment due to the nature of updates and inserts targeting columns specifically
  def encode[S <: SettableByName[S]](value: ScalaType, structure: S): S
}
object CqlEncoder extends UdtCollectionsForCqlEncoder with MagnoliaDerivationForCqlEncoder {
  def apply[A](implicit encoder: CqlEncoder[A]): CqlEncoder[A] = encoder

  implicit def cqlEncoderForTuple2[A, B](implicit
    columnEncoderForA: CqlColumnEncoder[A],
    columnEncoderForB: CqlColumnEncoder[B]
  ): CqlEncoder[(A, B)] =
    new CqlEncoder[(A, B)] {
      override def encode[S <: SettableByName[S]](value: (A, B), structure: S): S = {
        val withA = columnEncoderForA.encodeFieldByIndex(index = 0, value = value._1, structure = structure)
        val withB = columnEncoderForB.encodeFieldByIndex(index = 1, value = value._2, structure = withA)
        withB
      }
    }

  implicit def cqlEncoderForTuple3[A, B, C](implicit
    columnEncoderForA: CqlColumnEncoder[A],
    columnEncoderForB: CqlColumnEncoder[B],
    columnEncoderForC: CqlColumnEncoder[C]
  ): CqlEncoder[(A, B, C)] =
    new CqlEncoder[(A, B, C)] {
      override def encode[S <: SettableByName[S]](value: (A, B, C), structure: S): S = {
        val withA = columnEncoderForA.encodeFieldByIndex(index = 0, value = value._1, structure = structure)
        val withB = columnEncoderForB.encodeFieldByIndex(index = 1, value = value._2, structure = withA)
        val withC = columnEncoderForC.encodeFieldByIndex(index = 2, value = value._3, structure = withB)
        withC
      }
    }

  def fromUdtValue[A](f: (A, UdtValue) => UdtValue): CqlEncoder[A] =
    cqlEncoderViaCqlColumnEncoder(CqlColumnEncoder.udt(f))

  implicit def cqlEncoderViaCqlColumnEncoder[A](implicit columnEncoder: CqlColumnEncoder[A]): CqlEncoder[A] =
    new CqlEncoder[A] {
      override protected[virgil] def encodeByName[S <: SettableByName[S]](
        fieldName: String,
        value: A,
        structure: S
      ): S =
        columnEncoder.encodeFieldByName(fieldName, value, structure)

      override def encode[S <: SettableByName[S]](value: A, structure: S): S =
        columnEncoder.encodeFieldByIndex(index = 0, value = value, structure = structure)
    }
}

trait UdtCollectionsForCqlEncoder {
  implicit def cqlEncoderForOption[A <: Product](implicit
    cqlEncoderForA: CqlEncoder[A]
  ): CqlEncoder[Option[A]] =
    new CqlEncoder[Option[A]] {
      override protected[virgil] def encodeByName[S <: SettableByName[S]](
        fieldName: String,
        value: Option[A],
        structure: S
      ): S =
        value match {
          case Some(value) =>
            val initial = structure.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
            structure.setUdtValue(fieldName, cqlEncoderForA.encode(value, initial))

          case None =>
            structure.setToNull(fieldName)
        }

      override def encode[S <: SettableByName[S]](value: Option[A], structure: S): S =
        value match {
          case Some(a) => cqlEncoderForA.encode(a, structure)
          case None    => structure
        }
    }

  implicit def cqlEncoderForList[A <: Product](implicit element: CqlEncoder[A]): CqlEncoder[List[A]] =
    new CqlEncoder[List[A]] {
      override protected[virgil] def encodeByName[S <: SettableByName[S]](
        fieldName: String,
        value: List[A],
        structure: S
      ): S = {
        val listType   = structure.getType(fieldName).asInstanceOf[ListType]
        val elemType   = listType.getElementType.asInstanceOf[UserDefinedType]
        val initial    = elemType.newValue()
        val listDriver = value.map(scala => element.encode(scala, initial)).asJava
        structure.setList(fieldName, listDriver, classOf[UdtValue])
      }

      override def encode[S <: SettableByName[S]](value: List[A], structure: S): S = {
        val listType   = structure.getType(0).asInstanceOf[ListType]
        val elemType   = listType.getElementType.asInstanceOf[UserDefinedType]
        val initial    = elemType.newValue()
        val listDriver = value.map(scala => element.encode(scala, initial)).asJava
        structure.setList(0, listDriver, classOf[UdtValue])
      }
    }
}

trait MagnoliaDerivationForCqlEncoder {
  type Typeclass[T] = CqlEncoder[T]

  def join[T](ctx: CaseClass[CqlEncoder, T]): CqlEncoder[T] = new Typeclass[T] {
    override def encodeByName[S <: SettableByName[S]](name: String, value: T, structure: S): S = {
      val substructure = structure.getType(name).asInstanceOf[UserDefinedType].newValue()
      val completedSubstructure = ctx.parameters.foldLeft(substructure) { case (acc, p) =>
        val fieldValue = p.dereference(value)
        p.typeclass.encodeByName(p.label, fieldValue, acc)
      }

      structure.setUdtValue(name, completedSubstructure)
    }

    override def encode[S <: SettableByName[S]](value: T, structure: S): S =
      ctx.parameters.foldLeft(structure) { case (acc, p) =>
        val fieldValue = p.dereference(value)
        p.typeclass.encodeByName(p.label, fieldValue, acc)
      }
  }

  def derive[T]: CqlEncoder[T] = macro Magnolia.gen[T]
}
