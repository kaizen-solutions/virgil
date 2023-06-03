package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.SelectPageRow

trait SelectPageRowInstances:
  given cqlRowDecoderForSelectPageRow: CqlRowDecoder.Object[SelectPageRow] =
    CqlRowDecoder.derive[SelectPageRow]
