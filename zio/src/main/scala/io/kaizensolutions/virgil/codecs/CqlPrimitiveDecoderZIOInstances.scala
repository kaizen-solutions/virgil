package io.kaizensolutions.virgil.codecs

import io.kaizensolutions.virgil.codecs.CqlPrimitiveDecoder.ListPrimitiveDecoder
import zio.Chunk
import scala.jdk.CollectionConverters._

trait CqlPrimitiveDecoderZIOInstances {
  implicit def chunkCqlPrimitiveDecoder[A](implicit
    element: CqlPrimitiveDecoder[A]
  ): CqlPrimitiveDecoder.WithDriver[Chunk[A], java.util.List[element.DriverType]] =
    ListPrimitiveDecoder[Chunk, A, element.DriverType](
      element,
      (driverList, transformElement) => Chunk.fromIterable(driverList.asScala.map(transformElement))
    )
}
object CqlPrimitiveDecoderZIOInstances extends CqlPrimitiveDecoderZIOInstances
