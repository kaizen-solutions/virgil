package com.caesars.virgil.cql

import com.caesars.virgil.codecs.Reader
import com.caesars.virgil.{Action, Columns, Query}
import com.datastax.oss.driver.api.core.cql.Row

import scala.collection.immutable.ListMap

/**
 * Represents a fully formulated query that has been built by the Cql String
 * Interpolator and can be converted into an CassandraInteraction that can be
 * submitted to Cassandra for execution.
 */
final case class CqlInterpolatedString private (queryString: String, dataToBeBound: ListMap[String, ValueInCql]) {
  def query[Output](implicit evidence: Reader[Output]): Query[Output] =
    Query(
      query = queryString,
      columns = Columns.from(dataToBeBound),
      reader = evidence
    )

  def query: Query[Row] =
    Query(
      query = queryString,
      columns = Columns.from(dataToBeBound),
      reader = Reader.cassandraRowReader
    )

  def action: Action.Single =
    Action.Single(
      query = queryString,
      columns = Columns.from(dataToBeBound)
    )
}
