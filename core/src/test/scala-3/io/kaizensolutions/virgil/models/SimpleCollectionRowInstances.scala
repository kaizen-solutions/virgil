package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes.*

trait SimpleCollectionRowInstances:
  given cqlRowDecoderForSimpleCollectionRow: CqlRowDecoder.Object[SimpleCollectionRow] =
    CqlRowDecoder.derive[SimpleCollectionRow]
