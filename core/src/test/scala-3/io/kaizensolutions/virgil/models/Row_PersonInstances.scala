package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.Row_Person

trait Row_PersonInstances:
  given cqlRowDecoderForRow_Person: CqlRowDecoder.Object[Row_Person] = CqlRowDecoder.derive[Row_Person]
