package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.{CqlUdtValueDecoder, CqlUdtValueEncoder}
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_Data

trait UDT_DataInstances:
  given cqlUdtValueEncoderForUDT_Data: CqlUdtValueEncoder.Object[UDT_Data] = CqlUdtValueEncoder.derive[UDT_Data]
  given cqlUdtValueDecoderForUDT_Data: CqlUdtValueDecoder.Object[UDT_Data] = CqlUdtValueDecoder.derive[UDT_Data]
