package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.InsertBuilderSpecDatatypes.WriteTimeNameResult

trait WriteTimeNameResultInstances:
  given cqlRowDecoderForWriteTimeNameResult: CqlRowDecoder.Object[WriteTimeNameResult] =
    CqlRowDecoder.derive[WriteTimeNameResult]
