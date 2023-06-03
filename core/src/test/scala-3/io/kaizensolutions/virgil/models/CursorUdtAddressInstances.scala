package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.{CqlUdtValueDecoder, CqlUdtValueEncoder}
import io.kaizensolutions.virgil.models.CursorSpecDatatypes.CursorUdtAddress

trait CursorUdtAddressInstances:
  given cqlUdtValueDecoderForCursorUdtAddress: CqlUdtValueDecoder.Object[CursorUdtAddress] =
    CqlUdtValueDecoder.derive[CursorUdtAddress]

  given cqlUdtValueEncoderForCursorUdtAddress: CqlUdtValueEncoder.Object[CursorUdtAddress] =
    CqlUdtValueEncoder.derive[CursorUdtAddress]
