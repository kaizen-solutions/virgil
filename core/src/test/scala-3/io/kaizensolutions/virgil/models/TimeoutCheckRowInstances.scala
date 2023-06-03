package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.TimeoutCheckRow

trait TimeoutCheckRowInstances:
  given cqlRowDecoderForTimeoutCheckRow: CqlRowDecoder.Object[TimeoutCheckRow] =
    CqlRowDecoder.derive[TimeoutCheckRow]
