package io.kaizensolutions.virgil.cql

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.CQL
import io.kaizensolutions.virgil.MutationResult
import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.internal.BindMarkers

import scala.collection.immutable.ListMap
import scala.collection.mutable

/**
 * CqlInterpolatedString is an intermediate representation that can render
 * itself into the final representation that can be submitted to Cassandra for
 * execution.
 *
 * Please note Array is mutable and is used for performance.
 */
final case class CqlInterpolatedString(
  private[cql] val rawQueryRepresentation: Array[CqlPartRepr]
) {
  private[virgil] def render: (String, ListMap[String, ValueInCql]) = {
    val queryString         = new mutable.StringBuilder
    var markers             = ListMap.empty[String, ValueInCql]
    var markerCounter       = 0
    val parameterNamePrefix = "param"
    val colon               = ":"

    rawQueryRepresentation.foreach {
      case CqlPartRepr.Pair(query, marker) =>
        val param = s"$parameterNamePrefix${markerCounter}"
        queryString.append(query + colon + param)
        markers = markers + (param -> marker)
        markerCounter += 1

      case CqlPartRepr.Query(query) =>
        queryString.append(query)

      case CqlPartRepr.Marker(marker) =>
        val param = s"$parameterNamePrefix${markerCounter}"
        queryString.append(colon + param)
        markers = markers + (param -> marker)
        markerCounter += 1
    }

    (queryString.toString(), markers)
  }

  def mutation: CQL[MutationResult] = {
    val (queryString, dataToBeBound) = render
    CQL.cqlMutation(queryString, BindMarkers.from(dataToBeBound))
  }

  def query[Output](implicit evidence: CqlRowDecoder.Object[Output]): CQL[Output] = {
    val (queryString, dataToBeBound) = render
    CQL.cqlQuery(queryString, BindMarkers.from(dataToBeBound))
  }

  def query: CQL[Row] =
    query(CqlRowDecoder.cqlRowDecoderForUnderlyingRow)

  def ++(that: CqlInterpolatedString): CqlInterpolatedString =
    CqlInterpolatedString(rawQueryRepresentation ++ that.rawQueryRepresentation)

  def appendString(that: String): CqlInterpolatedString =
    CqlInterpolatedString(rawQueryRepresentation :+ CqlPartRepr.Query(that))

  def stripMargin: CqlInterpolatedString =
    copy(rawQueryRepresentation = rawQueryRepresentation.map {
      case CqlPartRepr.Pair(query, marker) => CqlPartRepr.Pair(query.stripMargin, marker)
      case CqlPartRepr.Query(query)        => CqlPartRepr.Query(query.stripMargin)
      case m @ CqlPartRepr.Marker(_)       => m
    })
}
trait CqlInterpolatedStringSyntax {
  implicit class CqlInterpolatedStringOpsForString(self: String) {
    def appendCql(that: CqlInterpolatedString): CqlInterpolatedString =
      CqlInterpolatedString(CqlPartRepr.Query(self) +: that.rawQueryRepresentation)

    def asCql: CqlInterpolatedString =
      CqlInterpolatedString(Array(CqlPartRepr.Query(self)))
  }
}
