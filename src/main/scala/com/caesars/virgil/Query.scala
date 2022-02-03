package com.caesars.virgil

import com.caesars.virgil.codecs.Reader

/**
 * Queries map 1:1 with CQL SELECT statements and return a list of results which
 * can be viewed as any datatype having the Reader capability
 *
 * @param query
 *   is the raw formulated query using named markers if there is data to submit
 * @param columns
 *   is the data needed to submit to Cassandra
 * @param reader
 *   is the capability to read data from Cassandra
 * @tparam OutputType
 *   is the type of data we expect to read from Cassandra
 */
final case class Query[OutputType](query: String, columns: Columns, reader: Reader[OutputType]) {

  /**
   * In case you formulated an action as a query, where you don't get a sensible
   * return type from Cassandra, here's an opportunity to change it back
   */
  def toAction: Action = Action.Single(query, columns)

  def withOutput[OutputType2](implicit reader: Reader[OutputType2]): Query[OutputType2] =
    Query[OutputType2](query, columns, reader)
}
