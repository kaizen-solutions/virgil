package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.cql._
import io.kaizensolutions.virgil.dsl._
import io.kaizensolutions.virgil.models.MusicLibraryItem
import zio.Chunk
import zio.RIO
import zio.test._

object SelectBuilderSpec {
  def selectBuilderSpec =
    suite("Select Builder Specification") {
      test("Select builder replicates low level CQL") {
        val dslQuery: CQL[Row] = SelectBuilder
          .from("table")
          .columns("col1", "col2", "col3", "col4")
          .where("col1" === "value")
          .and("col2" === 2L)
          .and("col3" === 3.0)
          .buildRow

        val cqlQuery: CQL[Row] =
          cql"SELECT col1, col2, col3, col4 FROM table WHERE col1 = ${"value"} AND col2 = ${2L} AND col3 = ${3.0}".query

        val cqlQueryNormalizedRenderedQuery = cqlQuery.cqlType.debug
          .replace("param0", "col1_relation")
          .replace("param1", "col2_relation")
          .replace("param2", "col3_relation")

        assertTrue(dslQuery.cqlType.debug == cqlQueryNormalizedRenderedQuery)
      } +
        test("Select all songs by artist") {
          val artist                = "The Beatles"
          val allSongsByArtistQuery = SelectBuilder
            .from(MusicLibraryItem.tableName)
            .allColumns
            .where(MusicLibraryItem.ArtistName === artist)

          val rawQuery =
            (
              cql"SELECT * FROM " ++
                MusicLibraryItem.tableName.asCql ++
                cql" WHERE artist_name = $artist"
            ).query[MusicLibraryItem]

          selectTest(
            allSongsByArtistQuery,
            rawQuery
          )(queryResult => assertTrue(queryResult.size == 3))
        } +
        test("Select all songs from a specific album") {
          val artist            = "The Beatles"
          val album             = "Abbey Road"
          val allSongsFromAlbum = SelectBuilder
            .from(MusicLibraryItem.tableName)
            .allColumns
            .where(MusicLibraryItem.ArtistName === artist)
            .and(MusicLibraryItem.AlbumName === album)

          val rawQuery =
            (
              cql"SELECT * FROM " ++
                MusicLibraryItem.tableName.asCql ++
                cql" WHERE artist_name = $artist AND album_name = $album"
            ).query[MusicLibraryItem]

          selectTest(
            allSongsFromAlbum,
            rawQuery
          )(queryResult => assertTrue(queryResult.size == 2))
        } +
        test("Select a specific song") {
          val artist       = "The Beatles"
          val album        = "Abbey Road"
          val song         = "Come Together"
          val specificSong = SelectBuilder
            .from(MusicLibraryItem.tableName)
            .allColumns
            .where(MusicLibraryItem.ArtistName === artist)
            .and(MusicLibraryItem.AlbumName === album)
            .and(MusicLibraryItem.SongTitle === song)

          val rawQuery =
            (
              cql"SELECT * FROM " ++
                MusicLibraryItem.tableName.asCql ++
                cql" WHERE artist_name = $artist AND album_name = $album AND song_title = $song"
            ).query[MusicLibraryItem]

          selectTest(
            specificSong,
            rawQuery
          )(queryResult => assertTrue(queryResult.size == 1))
        } +
        test("Select specific songs in album") {
          val artist = "The Beatles"
          val album  = "Abbey Road"
          val songs  = List("Come Together", "Something")

          val specificSongs = SelectBuilder
            .from(MusicLibraryItem.tableName)
            .allColumns
            .where(MusicLibraryItem.ArtistName === artist)
            .and(MusicLibraryItem.AlbumName === album)
            .and(MusicLibraryItem.SongTitle in songs)

          val rawQuery =
            (
              cql"SELECT * FROM " ++
                MusicLibraryItem.tableName.asCql ++
                cql" WHERE artist_name = $artist AND album_name = $album AND song_title IN $songs"
            ).query[MusicLibraryItem]

          selectTest(
            specificSongs,
            rawQuery
          )(queryResult => assertTrue(queryResult.size == 2))
        }
    }

  private def selectTest[State <: SelectState.NonEmpty, A <: Product](
    dsl: SelectBuilder[State],
    rawQuery: CQL[A]
  )(assert: Chunk[A] => TestResult)(implicit ev: CqlRowDecoder.Object[A]): RIO[CQLExecutor, TestResult] = {
    val dslQuery = dsl.build[A]

    val queryResult    = CQLExecutor.execute(dslQuery).runCollect
    val rawQueryResult = CQLExecutor.execute(rawQuery).runCollect

    queryResult.zipWith(rawQueryResult)((queryResult, rawQueryResult) =>
      assertTrue(queryResult == rawQueryResult) && assert(queryResult)
    )
  }
}
