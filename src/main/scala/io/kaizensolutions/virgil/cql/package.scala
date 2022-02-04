package io.kaizensolutions.virgil

package object cql extends CqlInterpolatedStringSyntax {
  implicit class CqlStringContext(ctx: StringContext) {
    val cql = new CqlStringInterpolator(ctx)
  }
}
