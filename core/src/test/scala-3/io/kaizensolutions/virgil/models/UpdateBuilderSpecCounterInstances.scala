package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.UpdateBuilderSpecDatatypes.UpdateBuilderSpecCounter

trait UpdateBuilderSpecCounterInstances:
  given cqlRowDecoder: CqlRowDecoder.Object[UpdateBuilderSpecCounter] =
    CqlRowDecoder.derive[UpdateBuilderSpecCounter]
