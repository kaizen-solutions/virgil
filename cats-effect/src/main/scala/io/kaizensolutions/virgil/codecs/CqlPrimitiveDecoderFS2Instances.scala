package io.kaizensolutions.virgil.codecs

import fs2.Chunk
import io.kaizensolutions.virgil.codecs.CqlPrimitiveDecoder.ListPrimitiveDecoder

import scala.jdk.CollectionConverters._

trait CqlPrimitiveDecoderFS2Instances {
  implicit def chunkCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[Chunk[A], java.util.List[element.DriverType]] =
    ListPrimitiveDecoder[Chunk, A, element.DriverType](
      element,
      (driverList, transformElement) => Chunk.iterable(driverList.asScala.map(transformElement))
    )
}
object CqlPrimitiveDecoderFS2Instances extends CqlPrimitiveDecoderFS2Instances
