package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.PreparedStatementsResponse

trait PreparedStatementsResponseInstances:
  given cqlRowDecoderForPreparedStatementsResponse: CqlRowDecoder.Object[PreparedStatementsResponse] =
    CqlRowDecoder.derive[PreparedStatementsResponse]
