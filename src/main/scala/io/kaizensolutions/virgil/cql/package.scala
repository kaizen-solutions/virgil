package io.kaizensolutions.virgil

package object cql {
  implicit class CqlStringContext(ctx: StringContext) {
    val cql = new CqlStringInterpolator(ctx)
  }
}
