//package io.kaizensolutions.virgil.cql
//
//import com.datastax.oss.driver.api.core.cql.Row
//import io.kaizensolutions.virgil.codecs.Reader
//import io.kaizensolutions.virgil.{BindMarkers, Mutation, Query, QueryType}
//
//import scala.collection.immutable.ListMap
//
///**
// * Represents a fully formulated query that has been built by the Cql String
// * Interpolator and can be converted into an CassandraInteraction that can be
// * submitted to Cassandra for execution.
// */
//final case class CqlInterpolatedString private (queryString: String, dataToBeBound: ListMap[String, ValueInCql]) {
//  def mutation: Mutation.RawCql = Mutation.RawCql(queryString, BindMarkers.from(dataToBeBound))
//
//  def query[Output](implicit evidence: Reader[Output]): Query[Output] =
//    Query(
//      queryType = QueryType.RawCql(
//        query = queryString,
//        columns = BindMarkers.from(dataToBeBound)
//      ),
//      reader = evidence
//    )
//
//  def query: Query[Row] =
//    Query(
//      queryType = QueryType.RawCql(
//        query = queryString,
//        columns = BindMarkers.from(dataToBeBound)
//      ),
//      reader = Reader.cassandraRowReader
//    )
//
//  def ++(that: CqlInterpolatedString): CqlInterpolatedString =
//    CqlInterpolatedString(
//      queryString = s"${queryString}${that.queryString}",
//      dataToBeBound = dataToBeBound ++ that.dataToBeBound
//    )
//
//  def appendString(that: String): CqlInterpolatedString =
//    CqlInterpolatedString(
//      queryString = s"$queryString$that",
//      dataToBeBound = dataToBeBound
//    )
//}
//trait CqlInterpolatedStringSyntax {
//  implicit class CqlInterpolatedStringOpsForString(self: String) {
//    def appendCql(that: CqlInterpolatedString): CqlInterpolatedString =
//      CqlInterpolatedString(
//        queryString = s"$self${that.queryString}",
//        dataToBeBound = that.dataToBeBound
//      )
//  }
//}
