package io.kaizensolutions.virgil

/**
 * We consider the cql package to be the low-level API for this library where
 * power users can directly express their intent as close as they can to the CQL
 * syntax. For the higher level API which assists you in building queries and
 * actions, see the [[io.kaizensolutions.virgil.dsl]] package
 */
package object cql extends CqlInterpolatedStringSyntax {
  implicit class CqlStringContext(ctx: StringContext) {
    val cql = new CqlStringInterpolator(ctx)
  }
}
