package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.models.RelationSpecDatatypes.RelationSpec_Person

trait RelationSpec_PersonInstances:
  given cqlRowDecoderForRelationSpec_Person: CqlRowDecoder.Object[RelationSpec_Person] =
    CqlRowDecoder.derive[RelationSpec_Person]
