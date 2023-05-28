package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.{CqlUdtValueDecoder, CqlUdtValueEncoder}
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_ExampleCollectionNestedUDTType

trait UDT_ExampleCollectionNestedUDTTypeInstances:
  given cqlUdtValueEncoderForUDT_ExampleCollectionNestedUDTType
    : CqlUdtValueEncoder.Object[UDT_ExampleCollectionNestedUDTType] =
    CqlUdtValueEncoder.derive[UDT_ExampleCollectionNestedUDTType]

  given cqlUdtValueDecoderForUDT_ExampleCollectionNestedUDTType
    : CqlUdtValueDecoder.Object[UDT_ExampleCollectionNestedUDTType] =
    CqlUdtValueDecoder.derive[UDT_ExampleCollectionNestedUDTType]
