package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
import magnolia1._

trait RowEncoder[A] {
  def encodeByFieldName(structure: BoundStatementBuilder, fieldName: String, value: A): BoundStatementBuilder
  def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder
}
object RowEncoder extends RowEncoderMagnoliaDerivation {
  // If you want to encode an entire case class to a row rather than pieces (which is what RowEncoder does)
  trait Object[A] extends RowEncoder[A] {
    def encode(structure: BoundStatementBuilder, value: A): BoundStatementBuilder

    def encodeByFieldName(structure: BoundStatementBuilder, fieldName: String, value: A): BoundStatementBuilder =
      encode(structure, value)

    def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder =
      encode(structure, value)
  }

  def component[A](implicit encoder: RowEncoder[A]): RowEncoder[A] = encoder

  implicit def fromCqlPrimitiveEncoder[A](implicit prim: CqlPrimitiveEncoder[A]): RowEncoder[A] =
    new RowEncoder[A] {
      override def encodeByFieldName(
        structure: BoundStatementBuilder,
        fieldName: String,
        value: A
      ): BoundStatementBuilder = {
        val driverType  = structure.getType(fieldName)
        val driverValue = prim.scala2Driver(value, driverType)
        structure.set(fieldName, driverValue, prim.driverClass)
      }

      override def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder = {
        val driverType  = structure.getType(index)
        val driverValue = prim.scala2Driver(value, driverType)
        structure.set(index, driverValue, prim.driverClass)
      }
    }
}
trait RowEncoderMagnoliaDerivation {
  type Typeclass[T] = RowEncoder[T]

  def join[T](ctx: CaseClass[RowEncoder, T]): RowEncoder.Object[T] =
    new RowEncoder.Object[T] {
      override def encode(structure: BoundStatementBuilder, value: T): BoundStatementBuilder =
        ctx.parameters.foldLeft(structure) { (structure, param) =>
          val fieldName  = param.label
          val fieldValue = param.dereference(value)
          val encoder    = param.typeclass
          encoder.encodeByFieldName(structure, fieldName, fieldValue)
        }
    }

  // Semi-automatic derivation
  def entireRow[T]: RowEncoder.Object[T] = macro Magnolia.gen[T]
}
