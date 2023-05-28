package io.kaizensolutions.virgil.codecs

import io.kaizensolutions.virgil.codecs.CqlPrimitiveEncoder.ListPrimitiveEncoder
import fs2.Chunk

import scala.jdk.CollectionConverters._

trait CqlPrimitiveEncoderFS2Instances {
  implicit def chunkCqlPrimitiveEncoder[A](implicit
    element: CqlPrimitiveEncoder[A]
  ): ListPrimitiveEncoder[Chunk, A, element.DriverType] =
    ListPrimitiveEncoder[Chunk, A, element.DriverType](
      element,
      (chunk, transform) => chunk.map(transform).toList.asJava
    )
}

object CqlPrimitiveEncoderFS2Instances extends CqlPrimitiveEncoderFS2Instances
