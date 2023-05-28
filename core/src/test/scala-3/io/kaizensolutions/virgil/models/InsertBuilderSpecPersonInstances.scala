package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.InsertBuilderSpecDatatypes.InsertBuilderSpecPerson

trait InsertBuilderSpecPersonInstances:
  given cqlRowDecoderForInsertBuilderSpecPerson: CqlRowDecoder.Object[InsertBuilderSpecPerson] =
    CqlRowDecoder.derive[InsertBuilderSpecPerson]
