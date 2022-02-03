package com.caesars.virgil.cql

import com.caesars.virgil.{CassandraInteraction, Column, Columns}
import com.caesars.virgil.codecs.Reader

import scala.collection.immutable.ListMap

/**
 * Represents a fully formulated query that has been built by the Cql String
 * Interpolator and can be converted into an CassandraInteraction that can be
 * submitted to Cassandra for execution.
 */
final case class CqlInterpolatedString private (queryString: String, dataToBeBound: ListMap[String, ValueInCql]) {
  def query[Output](implicit evidence: Reader[Output]): CassandraInteraction.Query[Output] =
    CassandraInteraction.Query(
      query = queryString,
      columns = Columns.from(dataToBeBound),
      reader = evidence
    )

  def action: CassandraInteraction.Action =
    CassandraInteraction.Action(
      query = queryString,
      columns = Columns.from(dataToBeBound)
    )
}
