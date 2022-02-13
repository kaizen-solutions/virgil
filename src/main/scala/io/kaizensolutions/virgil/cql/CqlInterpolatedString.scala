package io.kaizensolutions.virgil.cql

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.internal.BindMarkers
import io.kaizensolutions.virgil.{CQL, MutationResult}

import scala.collection.immutable.ListMap

/**
 * Represents a fully formulated query that has been built by the Cql String
 * Interpolator and can be converted into an CassandraInteraction that can be
 * submitted to Cassandra for execution.
 */
final case class CqlInterpolatedString private (
  private[cql] val queryString: String,
  private[cql] val dataToBeBound: ListMap[String, ValueInCql]
) {
  def mutation: CQL[MutationResult] = CQL.cqlMutation(queryString, BindMarkers.from(dataToBeBound))

  def query[Output](implicit evidence: Reader[Output]): CQL[Output] =
    CQL.cqlQuery(queryString, BindMarkers.from(dataToBeBound))

  def query: CQL[Row] =
    CQL.cqlQuery(queryString, BindMarkers.from(dataToBeBound))(Reader.cassandraRowReader)

  def ++(that: CqlInterpolatedString): CqlInterpolatedString =
    CqlInterpolatedString(
      queryString = s"${queryString}${that.queryString}",
      dataToBeBound = dataToBeBound ++ that.dataToBeBound
    )

  def appendString(that: String): CqlInterpolatedString =
    CqlInterpolatedString(
      queryString = s"$queryString$that",
      dataToBeBound = dataToBeBound
    )
}
trait CqlInterpolatedStringSyntax {
  implicit class CqlInterpolatedStringOpsForString(self: String) {
    def appendCql(that: CqlInterpolatedString): CqlInterpolatedString =
      CqlInterpolatedString(
        queryString = s"$self${that.queryString}",
        dataToBeBound = that.dataToBeBound
      )

    def asCql: CqlInterpolatedString = CqlInterpolatedString(queryString = self, dataToBeBound = ListMap.empty)
  }
}
