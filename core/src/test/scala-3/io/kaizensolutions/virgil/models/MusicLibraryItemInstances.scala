package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.codecs.CqlRowDecoder

trait MusicLibraryItemInstances:
  given cqlRowDecoderForMusicLibraryItem: CqlRowDecoder.Object[MusicLibraryItem] =
    CqlRowDecoder.derive[MusicLibraryItem]
