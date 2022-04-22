package io.kaizensolutions.virgil.cql

import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder

trait ValueInCqlInstances:
  given conversionToValueInCql[Scala](using encoder: CqlRowComponentEncoder[Scala]): Conversion[Scala, ValueInCql] =
    in =>
      new ValueInCql:
        type ScalaType = Scala
        val value: Scala                          = in
        val writer: CqlRowComponentEncoder[Scala] = encoder
