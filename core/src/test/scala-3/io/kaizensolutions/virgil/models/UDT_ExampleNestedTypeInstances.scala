package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlUdtValueDecoder
import io.kaizensolutions.virgil.codecs.CqlUdtValueEncoder
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_ExampleNestedType

trait UDT_ExampleNestedTypeInstances:
  given cqlUdtValueEncoderForUDT_ExampleNestedType: CqlUdtValueEncoder.Object[UDT_ExampleNestedType] =
    CqlUdtValueEncoder.derive[UDT_ExampleNestedType]

  given cqlUdtValueDecoderForUDT_ExampleNestedType: CqlUdtValueDecoder.Object[UDT_ExampleNestedType] =
    CqlUdtValueDecoder.derive[UDT_ExampleNestedType]
