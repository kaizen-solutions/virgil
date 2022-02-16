package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.ColumnEncoder
import io.kaizensolutions.virgil.internal.BindMarkerName

sealed trait Relation
object Relation extends RelationSyntax {
  final case class Binary[A](
    columnName: BindMarkerName,
    operator: BinaryOperator,
    value: A,
    writer: ColumnEncoder[A]
  ) extends Relation
  final case class IsNotNull(columnName: BindMarkerName) extends Relation
  final case class IsNull(columnName: BindMarkerName)    extends Relation
}
trait RelationSyntax {

  /**
   * Allows you you to write relations using a nicer syntax, some examples:
   *   - "a" > 1
   *   - "a" === 1
   *   - "a" < 1
   *   - "a" >= 1
   *   - "a" <= 1
   *   - "a" =!= 1
   *
   * @param rawColumn
   *   is the name of the column that is in the relation
   */
  implicit class RelationOps(rawColumn: String) {
    private val column = BindMarkerName.make(rawColumn)

    def ===[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.Equal, value, ev)

    def =!=[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.NotEqual, value, ev)

    def >[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.GreaterThan, value, ev)

    def >=[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.GreaterThanOrEqual, value, ev)

    def <[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.LessThan, value, ev)

    def <=[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.LessThanOrEqual, value, ev)

    def like[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.Like, value, ev)

    def in[A](value: A)(implicit ev: ColumnEncoder[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.In, value, ev)

    def isNotNull: Relation.IsNotNull =
      Relation.IsNotNull(column)
  }
}
