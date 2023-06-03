package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.UpdateBuilderSpecDatatypes.UpdateBuilderSpecPerson

trait UpdateBuilderSpecPersonInstances:
  given cqlRowDecoderUpdateBuilderSpecPerson: CqlRowDecoder.Object[UpdateBuilderSpecPerson] =
    CqlRowDecoder.derive[UpdateBuilderSpecPerson]
