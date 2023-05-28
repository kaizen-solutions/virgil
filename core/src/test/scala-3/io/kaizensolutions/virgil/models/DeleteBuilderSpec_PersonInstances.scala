package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.DeleteBuilderSpecDatatypes.DeleteBuilderSpec_Person

trait DeleteBuilderSpec_PersonInstances:
  given cqlRowDecoderForDeleteBuilderSpec_Person: CqlRowDecoder.Object[DeleteBuilderSpec_Person] =
    CqlRowDecoder.derive[DeleteBuilderSpec_Person]
