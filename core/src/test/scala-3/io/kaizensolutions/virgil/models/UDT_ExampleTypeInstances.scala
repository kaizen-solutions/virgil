package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlUdtValueDecoder
import io.kaizensolutions.virgil.codecs.CqlUdtValueEncoder
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_ExampleType

trait UDT_ExampleTypeInstances:
  given cqlUdtValueEncoderForUDT_ExampleType: CqlUdtValueEncoder.Object[UDT_ExampleType] =
    CqlUdtValueEncoder.derive[UDT_ExampleType]

  given cqlUdtValueDecoderForUDT_ExampleType: CqlUdtValueDecoder.Object[UDT_ExampleType] =
    CqlUdtValueDecoder.derive[UDT_ExampleType]
