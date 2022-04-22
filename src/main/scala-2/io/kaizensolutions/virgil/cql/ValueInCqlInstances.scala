package io.kaizensolutions.virgil.cql

import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder

trait ValueInCqlInstances {

  /**
   * This implicit conversion automatically captures the value and evidence of
   * the type's Writer in a cql interpolated string that is necessary to write
   * data into the Datastax statement
   */
  implicit def toValueInCql[Scala](in: Scala)(implicit encoder: CqlRowComponentEncoder[Scala]): ValueInCql =
    new ValueInCql {
      type ScalaType = Scala
      val value: Scala                          = in
      val writer: CqlRowComponentEncoder[Scala] = encoder
    }
}
