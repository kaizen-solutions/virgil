package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlUdtValueDecoder
import io.kaizensolutions.virgil.models.CursorSpecDatatypes.CursorUdtNote

trait CursorUdtNoteInstances {
  given cqlUdtValueDecoderForCursorUdtNote: CqlUdtValueDecoder.Object[CursorUdtNote] =
    CqlUdtValueDecoder.derive[CursorUdtNote]
}
