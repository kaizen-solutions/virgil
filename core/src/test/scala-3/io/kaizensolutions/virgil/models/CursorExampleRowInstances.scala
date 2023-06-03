package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CursorSpecDatatypes.CursorExampleRow

trait CursorExampleRowInstances:
  given cqlRowDecoderForCursorExampleRow: CqlRowDecoder.Object[CursorExampleRow] =
    CqlRowDecoder.derive[CursorExampleRow]
