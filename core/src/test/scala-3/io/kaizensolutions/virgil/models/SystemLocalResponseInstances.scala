package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.SystemLocalResponse

trait SystemLocalResponseInstances:
  given cqlRowDecoderForSystemLocalResponse: CqlRowDecoder.Object[SystemLocalResponse] =
    CqlRowDecoder.derive[SystemLocalResponse]
