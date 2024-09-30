package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlUdtValueDecoder
import io.kaizensolutions.virgil.codecs.CqlUdtValueEncoder
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_Address

trait UDT_AddressInstances {
  given cqlUdtValueEncoderForUDT_Address: CqlUdtValueEncoder.Object[UDT_Address] =
    CqlUdtValueEncoder.derive[UDT_Address]

  given cqlUdtValueDecoderForUDT_Address: CqlUdtValueDecoder.Object[UDT_Address] =
    CqlUdtValueDecoder.derive[UDT_Address]
}
