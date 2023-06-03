package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.UserDefinedTypesSpecDatatypes.Row_HeavilyNestedUDTTable

trait Row_HeavilyNestedUDTTableInstances:
  given cqlRowDecoderForRow_HeavilyNestedUDTTable: CqlRowDecoder.Object[Row_HeavilyNestedUDTTable] =
    CqlRowDecoder.derive[Row_HeavilyNestedUDTTable]
