package io.kaizensolutions.virgil.models

import io.kaizensolutions.virgil.annotations.CqlColumn

import java.time.LocalDate

final case class MusicLibraryItem(
  @CqlColumn(MusicLibraryItem.ArtistName) artistName: String,
  @CqlColumn(MusicLibraryItem.AlbumName) albumName: String,
  @CqlColumn(MusicLibraryItem.SongTitle) songTitle: String,
  @CqlColumn(MusicLibraryItem.ReleaseDate) releaseDate: LocalDate,
  @CqlColumn(MusicLibraryItem.Genre) genre: String,
  @CqlColumn(MusicLibraryItem.DurationSeconds) durationSeconds: Int
)
object MusicLibraryItem extends MusicLibraryItemInstances {
  val tableName       = "selectspec_musiclibrary"
  val ArtistName      = "artist_name"
  val AlbumName       = "album_name"
  val SongTitle       = "song_title"
  val ReleaseDate     = "release_date"
  val Genre           = "genre"
  val DurationSeconds = "duration_seconds"
}
