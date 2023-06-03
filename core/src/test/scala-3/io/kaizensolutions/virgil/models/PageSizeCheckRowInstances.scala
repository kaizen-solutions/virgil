package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.PageSizeCheckRow

trait PageSizeCheckRowInstances:
  given cqlRowDecoderForPageSizeCheckRow: CqlRowDecoder.Object[PageSizeCheckRow] =
    CqlRowDecoder.derive[PageSizeCheckRow]
