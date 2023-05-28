package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.CollectionsSpecDatatypes.*

trait OptionCollectionRowInstances:
  given cqlRowDecoderForOptionCollectionRow: CqlRowDecoder.Object[OptionCollectionRow] =
    CqlRowDecoder.derive[OptionCollectionRow]
