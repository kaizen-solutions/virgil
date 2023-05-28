package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
trait CursorExampleRowChunkVariantInstances:
  given cqlRowDecoderForCursorExampleRowChunkVariant: CqlRowDecoder.Object[CursorExampleRowChunkVariant] =
    CqlRowDecoder.derive[CursorExampleRowChunkVariant]
