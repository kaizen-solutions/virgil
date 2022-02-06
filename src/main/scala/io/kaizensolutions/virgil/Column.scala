package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Writer
import io.kaizensolutions.virgil.cql.ValueInCql

import scala.collection.immutable.ListMap

/**
 * A Column consists of a name along with the data and the capability to write
 * the data to a DataStax statement.
 */
trait Column { self =>
  type ScalaType
  def name: ColumnName
  def value: ScalaType
  def write: Writer[ScalaType]

  override def toString: String =
    s"Column(name = ${name.name}, value = $value)"
}
object Column {
  def make[A](columnName: ColumnName, columnValue: A)(implicit evidence: Writer[A]): Column =
    new Column {
      type ScalaType = A
      def name: ColumnName = columnName
      def value: A         = columnValue
      def write: Writer[A] = evidence
    }

  def from(valueInCql: ValueInCql, columnName: ColumnName): Column =
    new Column {
      type ScalaType = valueInCql.ScalaType
      def name: ColumnName         = columnName
      def value: ScalaType         = valueInCql.value
      def write: Writer[ScalaType] = valueInCql.writer
    }

  def withName(columnName: ColumnName, existing: Column): Column = new Column {
    override type ScalaType = existing.ScalaType
    override def name: ColumnName         = columnName
    override def value: ScalaType         = existing.value
    override def write: Writer[ScalaType] = existing.write
  }
}

final case class Columns(underlying: ListMap[ColumnName, Column]) {
  def +(column: Column): Columns =
    copy(underlying = underlying + (column.name -> column))

  def ++(that: Columns): Columns =
    copy(underlying = underlying ++ that.underlying)

  override def toString: String =
    s"""Columns(${underlying.map { case (k, v) => s"${k.name} -> ${v.value}" }.mkString(", ")})"""
}
object Columns {
  def from(columns: ListMap[String, ValueInCql]): Columns =
    Columns(columns.map { case (name, value) =>
      val colName = ColumnName.make(name)
      colName -> Column.from(value, colName)
    })

  def empty: Columns = Columns(ListMap.empty)
}

/**
 * The name of a column in a Cassandra table.
 */
final class ColumnName(val name: String) extends AnyVal
object ColumnName {
  def make(name: String): ColumnName = new ColumnName(name)
}
