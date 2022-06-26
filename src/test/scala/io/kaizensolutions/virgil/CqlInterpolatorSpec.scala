package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.cql._
import zio.test._

object CqlInterpolatorSpec {
  def cqlInterpolatorSpec =
    suite("CQL Interpolator specification") {
      test("can formulate a query without any bind markers") {
        val (queryString, bindMarkers) = cql"SELECT * FROM system.local".render
        assertTrue(queryString == "SELECT * FROM system.local") &&
        assertTrue(bindMarkers.isEmpty)
      } +
        test("can formulate a query with a bind marker") {
          val local                      = "local"
          val (queryString, bindMarkers) = cql"SELECT * FROM system.local WHERE key = $local".render
          assertTrue(queryString == "SELECT * FROM system.local WHERE key = :param0") &&
          assertTrue(bindMarkers.size == 1) &&
          assertTrue(bindMarkers.contains("param0")) &&
          assertTrue(bindMarkers("param0").value.asInstanceOf[String] == local)
        } +
        test("can formulate a hybrid query combining string and cql interpolation") {
          val local                      = "local"
          val table                      = "system.local"
          val (queryString, bindMarkers) = (cql"SELECT * FROM " ++ table.asCql ++ cql" WHERE key = $local").render
          assertTrue(queryString == "SELECT * FROM system.local WHERE key = :param0") &&
          assertTrue(bindMarkers.size == 1) &&
          assertTrue(bindMarkers.contains("param0")) &&
          assertTrue(bindMarkers("param0").value.asInstanceOf[String] == local)
        } +
        test("can compose queries containing bind markers together") {
          val idColumn   = "id"
          val nameColumn = "name"
          val tableName  = "persons"

          val personId   = 1
          val personName = "cal"

          val query =
            s"SELECT $idColumn, $nameColumn FROM $tableName ".asCql ++
              cql"WHERE ".appendString(idColumn) ++ cql" = $personId " ++
              cql"AND " ++ nameColumn.asCql ++ cql" = $personName"

          val (queryString, bindMarkers) = query.render
          assertTrue(
            queryString == s"SELECT $idColumn, $nameColumn FROM $tableName WHERE $idColumn = :param0 AND $nameColumn = :param1"
          ) &&
          assertTrue(bindMarkers.contains("param0") && bindMarkers.contains("param1")) &&
          assertTrue(bindMarkers("param0").value.asInstanceOf[Int] == personId) &&
          assertTrue(bindMarkers("param1").value.asInstanceOf[String] == personName)
        } +
        test("append CQL to a string") {
          val idColumn                     = "id"
          val nameColumn                   = "name"
          val tableName                    = "persons"
          val id                           = 1
          val selectFrom: String           = s"SELECT $idColumn, $nameColumn FROM $tableName "
          val where: CqlInterpolatedString = s"WHERE $idColumn = ".asCql ++ cql"$id"
          val query                        = selectFrom.appendCql(where)
          val (queryString, bindMarkers)   = query.render
          assertTrue(queryString == s"SELECT $idColumn, $nameColumn FROM $tableName WHERE $idColumn = :param0") &&
          assertTrue(bindMarkers.contains("param0")) &&
          assertTrue(bindMarkers("param0").value.asInstanceOf[Int] == id)
        } +
        test("stripMargin removes | in cql interpolated strings but leaves bind markers as is") {
          val query =
            cql"""SELECT id, name, persons
                 |FROM persons
                 |WHERE id = ${1} AND name = ${"cal"}""".stripMargin
          val (queryString, bindMarkers) = query.render
          val expected =
            """SELECT id, name, persons
              |FROM persons
              |WHERE id = :param0 AND name = :param1""".stripMargin

          assertTrue(queryString == expected) &&
          assertTrue(bindMarkers.size == 2) &&
          assertTrue(bindMarkers("param0").value.asInstanceOf[Int] == 1) &&
          assertTrue(bindMarkers("param1").value.asInstanceOf[String] == "cal")
        }
    }
}
