package io.kaizensolutions.virgil.codecs

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
trait CqlRowComponentEncoder[-A] { self =>
  def encodeByFieldName(structure: BoundStatementBuilder, fieldName: String, value: A): BoundStatementBuilder
  def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder

  def contramap[B](f: B => A): CqlRowComponentEncoder[B] = new CqlRowComponentEncoder[B] {
    override def encodeByFieldName(
      structure: BoundStatementBuilder,
      fieldName: String,
      value: B
    ): BoundStatementBuilder =
      self.encodeByFieldName(structure, fieldName, f(value))

    override def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: B): BoundStatementBuilder =
      self.encodeByIndex(structure, index, f(value))
  }
}
object CqlRowComponentEncoder {
  def apply[A](implicit encoder: CqlRowComponentEncoder[A]): CqlRowComponentEncoder[A] = encoder

  implicit def fromCqlPrimitiveEncoder[A](implicit prim: CqlPrimitiveEncoder[A]): CqlRowComponentEncoder[A] =
    new CqlRowComponentEncoder[A] {
      override def encodeByFieldName(
        structure: BoundStatementBuilder,
        fieldName: String,
        value: A
      ): BoundStatementBuilder =
        CqlPrimitiveEncoder.encodePrimitiveByFieldName(structure, fieldName, value)

      override def encodeByIndex(structure: BoundStatementBuilder, index: Int, value: A): BoundStatementBuilder =
        CqlPrimitiveEncoder.encodePrimitiveByIndex(structure, index, value)
    }
}
