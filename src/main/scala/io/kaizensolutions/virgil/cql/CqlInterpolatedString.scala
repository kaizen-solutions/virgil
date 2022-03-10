package io.kaizensolutions.virgil.cql

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.internal.BindMarkers
import io.kaizensolutions.virgil.{CQL, MutationResult}
import zio.Chunk

import scala.collection.immutable.ListMap

/**
 * CqlInterpolatedString is an intermediate representation that can render
 * itself into the final representation that can be submitted to Cassandra for
 * execution.
 */
final case class CqlInterpolatedString private (
  private[cql] val rawQueryRepresentation: Chunk[CqlPartRepr]
) {
  private[virgil] def render: (String, ListMap[String, ValueInCql]) = {
    val queryString         = new StringBuilder
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
}
trait CqlInterpolatedStringSyntax {
  implicit class CqlInterpolatedStringOpsForString(self: String) {
    def appendCql(that: CqlInterpolatedString): CqlInterpolatedString =
      CqlInterpolatedString(CqlPartRepr.Query(self) +: that.rawQueryRepresentation)

    def asCql: CqlInterpolatedString =
      CqlInterpolatedString(Chunk.single(CqlPartRepr.Query(self)))
  }
}
