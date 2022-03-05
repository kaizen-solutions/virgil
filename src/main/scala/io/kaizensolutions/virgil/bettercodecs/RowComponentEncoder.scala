package io.kaizensolutions.virgil.bettercodecs

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder

/**
 * Encodes a component of a row. For example if your Row consists of
 *
 * Row:
 *   - a: Int
 *   - b: String
 *   - c: UdtValue
 *
 * Then a, b, c are components of the row and you would have a RowEncoder[Int]
 * for `a`, a RowEncoder[String] for `b`, and a RowEncoder[UdtValue] for `c`.
 * @tparam A
 *   is the component to be encoded into the Row
 */
trait RowComponentEncoder[A] {
  def encodeByFieldName(structure: BoundStatementBuilder, fieldName: String, value: A): BoundStatementBuilder
  def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder
}
object RowComponentEncoder {
  def apply[A](implicit encoder: RowComponentEncoder[A]): RowComponentEncoder[A] = encoder

  implicit def fromCqlPrimitiveEncoder[A](implicit prim: CqlPrimitiveEncoder[A]): RowComponentEncoder[A] =
    new RowComponentEncoder[A] {
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
