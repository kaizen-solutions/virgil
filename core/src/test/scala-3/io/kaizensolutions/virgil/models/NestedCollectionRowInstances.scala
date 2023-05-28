package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes.*

trait NestedCollectionRowInstances:
  given cqlRowDecoderForNestedCollectionRow: CqlRowDecoder.Object[NestedCollectionRow] =
    CqlRowDecoder.derive[NestedCollectionRow]
