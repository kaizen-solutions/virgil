package io.kaizensolutions.virgil.cql

/**
 * CqlPartRepr is the internal representation of a CqlInterpolatedString and is
 * used to make composition of CqlInterpolatedStrings easier to cope with
 */
sealed private[cql] trait CqlPartRepr
private[cql] object CqlPartRepr {
  final case class Pair(query: String, marker: ValueInCql) extends CqlPartRepr
  final case class Query(query: String)                    extends CqlPartRepr
  final case class Marker(marker: ValueInCql)              extends CqlPartRepr
}
