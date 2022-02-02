package com.caesars.virgil

package object cql {
  implicit class CqlStringContext(ctx: StringContext) {
    val cql = new CqlStringInterpolator(ctx)
  }
}
