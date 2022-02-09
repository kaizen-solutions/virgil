package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Writer
import io.kaizensolutions.virgil.cql.ValueInCql

import scala.collection.immutable.ListMap

/**
 * A BindMarker consists of a name along with the data and the capability to
 * write the data to a DataStax statement.
 */
trait BindMarker { self =>
  type ScalaType
  def name: BindMarkerName
  def value: ScalaType
  def write: Writer[ScalaType]

  override def toString: String =
    s"Column(name = ${name.name}, value = $value)"
}
object BindMarker {
  def make[A](columnName: BindMarkerName, columnValue: A)(implicit evidence: Writer[A]): BindMarker =
    new BindMarker {
      type ScalaType = A
      def name: BindMarkerName = columnName
      def value: A             = columnValue
      def write: Writer[A]     = evidence
    }

  def from(valueInCql: ValueInCql, columnName: BindMarkerName): BindMarker =
    new BindMarker {
      type ScalaType = valueInCql.ScalaType
      def name: BindMarkerName     = columnName
      def value: ScalaType         = valueInCql.value
      def write: Writer[ScalaType] = valueInCql.writer
    }

  def withName(columnName: BindMarkerName, existing: BindMarker): BindMarker = new BindMarker {
    override type ScalaType = existing.ScalaType
    override def name: BindMarkerName     = columnName
    override def value: ScalaType         = existing.value
    override def write: Writer[ScalaType] = existing.write
  }
}

final case class BindMarkers(underlying: ListMap[BindMarkerName, BindMarker]) {
  def +(column: BindMarker): BindMarkers =
    copy(underlying = underlying + (column.name -> column))

  def ++(that: BindMarkers): BindMarkers =
    copy(underlying = underlying ++ that.underlying)

  override def toString: String =
    s"""BindMarkers(${underlying.map { case (k, v) => s"${k.name} -> ${v.value}" }.mkString(", ")})"""
}
object BindMarkers {
  def from(columns: ListMap[String, ValueInCql]): BindMarkers =
    BindMarkers(columns.map { case (name, value) =>
      val markerName = BindMarkerName.make(name)
      markerName -> BindMarker.from(value, markerName)
    })

  def empty: BindMarkers = BindMarkers(ListMap.empty)
}

/**
 * The name of a column in a Cassandra table.
 */
final class BindMarkerName(val name: String) extends AnyVal
object BindMarkerName {
  def make(name: String): BindMarkerName = new BindMarkerName(name)
}
