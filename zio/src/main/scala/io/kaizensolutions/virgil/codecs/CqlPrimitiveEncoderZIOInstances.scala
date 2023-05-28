package io.kaizensolutions.virgil.codecs

import io.kaizensolutions.virgil.codecs.CqlPrimitiveEncoder._
import zio.Chunk
import scala.jdk.CollectionConverters._

trait CqlPrimitiveEncoderZIOInstances {
  implicit def chunkCqlPrimitiveEncoder[ScalaElem](implicit
    element: CqlPrimitiveEncoder[ScalaElem]
  ): ListPrimitiveEncoder[Chunk, ScalaElem, element.DriverType] =
    ListPrimitiveEncoder[Chunk, ScalaElem, element.DriverType](
      element,
      (chunk, transform) => chunk.map(transform).asJava
    )
}

object CqlPrimitiveEncoderZIOInstances extends CqlPrimitiveEncoderZIOInstances
