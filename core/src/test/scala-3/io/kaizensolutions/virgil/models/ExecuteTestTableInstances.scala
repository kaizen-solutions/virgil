package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CqlExecutorSpecDatatypes.ExecuteTestTable

trait ExecuteTestTableInstances:
  given cqlRowDecoderForExecuteTestTable: CqlRowDecoder.Object[ExecuteTestTable] =
    CqlRowDecoder.derive[ExecuteTestTable]
