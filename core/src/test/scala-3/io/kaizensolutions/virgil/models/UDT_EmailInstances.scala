package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.{CqlUdtValueDecoder, CqlUdtValueEncoder}
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.UDT_Email

trait UDT_EmailInstances:
  given cqlUdtValueEncoderForUDT_Email: CqlUdtValueEncoder.Object[UDT_Email] = CqlUdtValueEncoder.derive[UDT_Email]
  given cqlUdtValueDecoderForUDT_Email: CqlUdtValueDecoder.Object[UDT_Email] = CqlUdtValueDecoder.derive[UDT_Email]
