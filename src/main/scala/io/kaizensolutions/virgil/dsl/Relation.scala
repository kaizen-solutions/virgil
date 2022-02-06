package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.{Column, ColumnName, Columns}
import io.kaizensolutions.virgil.codecs.Writer
import zio.Chunk

sealed trait Relation
object Relation extends RelationSyntax {
  final case class Binary[A](
    columnName: ColumnName,
    operator: BinaryOperator,
    value: A,
    writer: Writer[A]
  ) extends Relation
  final case class IsNotNull(columnName: ColumnName) extends Relation

  /**
   * renderRelations renders a list of relations into a string along with the
   * data that needs to be inserted into the driver's statement
   *
   * For example Chunk("a" > 1, "b" === 2, "c" < 3) will become "WHERE a >
   * :a_relation AND b = :b_relation AND c < :c_relation" along with
   * Columns(a_relation -> ..., b_relation -> ..., c_relation -> ...)
   *
   * @param relations
   * @return
   */
  def renderRelations(relations: Chunk[Relation]): (String, Columns) = {
    val initial = (Chunk[String](), Columns.empty)
    val (exprChunk, columns) =
      relations.foldLeft(initial) { case ((accExpr, accColumns), relation) =>
        relation match {
          case Relation.Binary(columnName, operator, value, writer) =>
            // For example, where("col1" >= 1) becomes
            // "col1 >= :col1_relation" along with Columns("col1_relation" -> 1 with write capabilities)
            val param      = s"${columnName.name}_relation"
            val column     = Column.make(ColumnName.make(param), value)(writer)
            val expression = s"${columnName.name} ${operator.render} :$param"
            (accExpr :+ expression, accColumns + column)

          case Relation.IsNotNull(columnName) =>
            val expression = s"${columnName.name} IS NOT NULL"
            (accExpr :+ expression, accColumns)
        }
      }
    val relationExpr = "WHERE " ++ exprChunk.mkString(" AND ")
    (relationExpr, columns)
  }
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
    private val column = ColumnName.make(rawColumn)

    def ===[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.Equal, value, ev)

    def =!=[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.NotEqual, value, ev)

    def >[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.GreaterThan, value, ev)

    def >=[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.GreaterThanOrEqual, value, ev)

    def <[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.LessThan, value, ev)

    def <=[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.LessThanOrEqual, value, ev)

    def like[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.Like, value, ev)

    def in[A](value: A)(implicit ev: Writer[A]): Relation.Binary[A] =
      Relation.Binary(column, BinaryOperator.In, value, ev)

    def isNotNull: Relation.IsNotNull =
      Relation.IsNotNull(column)
  }
}
