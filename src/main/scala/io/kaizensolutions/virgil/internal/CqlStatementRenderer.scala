package io.kaizensolutions.virgil.internal

import io.kaizensolutions.virgil.CQLType
import io.kaizensolutions.virgil.CQLType.Mutation
import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.dsl.{Assignment, Relation}
import zio.{Chunk, NonEmptyChunk}

private[virgil] object CqlStatementRenderer {
  def render(in: CQLType.Mutation): (String, BindMarkers) =
    in match {
      case Mutation.Insert(tableName, columns) =>
        insert.render(tableName, columns)

      case Mutation.Update(tableName, assignments, relations) =>
        update.render(tableName, assignments, relations)

      case Mutation.Delete(tableName, relations) =>
        delete.render(tableName, relations)

      case Mutation.Truncate(tableName) =>
        truncate.render(tableName)

      case Mutation.RawCql(cql, bindMarkers) =>
        (cql, bindMarkers)
    }

  def render[FromCassandra](in: CQLType.Query[FromCassandra]): (String, BindMarkers) =
    in.queryType match {
      case QueryType.Select(tableName, columnNames, relations) =>
        select.render(tableName, columnNames, relations)

      case QueryType.RawCql(query, bindMarkers) =>
        (query, bindMarkers)
    }

  private object insert {
    def render(table: String, columns: BindMarkers): (String, BindMarkers) =
      (s"INSERT INTO $table ${renderColumnNames(columns)} VALUES ${renderInterpolatedValues(columns)}", columns)

    private def renderColumnNames(columns: BindMarkers): String =
      columns.underlying.keys
        .map(_.name)
        .mkString(start = "(", sep = ",", end = ")")

    private def renderInterpolatedValues(columns: BindMarkers): String =
      columns.underlying.keys
        .map(colName => s":${colName.name}")
        .mkString(start = "(", sep = ",", end = ")")
  }

  private object update {
    def render(
      table: String,
      assignments: NonEmptyChunk[Assignment],
      relations: NonEmptyChunk[Relation]
    ): (String, BindMarkers) = {
      val (assignmentCql, assignmentColumns) = renderAssignments(assignments)
      val (relationsCql, relationsColumns)   = renderRelations(relations)
      val allColumns                         = assignmentColumns ++ relationsColumns

      (s"UPDATE $table $assignmentCql $relationsCql", allColumns)
    }

    private def renderAssignments(assignments: NonEmptyChunk[Assignment]): (String, BindMarkers) = {
      import Assignment._

      val renderedAssignments: NonEmptyChunk[(String, BindMarkers)] =
        assignments.map {
          // column name: example_column
          case AssignValue(columnName, value, ev) =>
            // example_column = :example_column
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $parameter"
            val column        = BindMarker.make(columnName, value)(ev)
            (queryString, BindMarkers.empty + column)

          case UpdateCounter(columnName, offset) =>
            // example_column + :example_column
            val sign          = if (offset > 0) "+" else "-"
            val absOffset     = offset.abs
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName $sign $parameter"
            val column        = BindMarker.make(columnName, absOffset)(CqlRowComponentEncoder[Long])
            (queryString, BindMarkers.empty + column)

          case p: PrependListItems[a] =>
            // :example_column + example_column
            import p._
            val rawColumnName = columnName.name
            val valuesToAdd   = values.toList
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $parameter + :$rawColumnName"
            val column        = BindMarker.make(columnName, valuesToAdd)(ev)
            (queryString, BindMarkers.empty + column)

          case RemoveListItems(columnName, values, ev) =>
            // example_column - :example_column
            val rawColumnName  = columnName.name
            val valuesToRemove = values.toList
            val parameter      = s":$rawColumnName"
            val queryString    = s"$rawColumnName = $rawColumnName - $parameter"
            val column         = BindMarker.make(columnName, valuesToRemove)(ev)
            (queryString, BindMarkers.empty + column)

          case a: AppendListItems[elem] =>
            // example_column + :example_column
            import a._
            val rawColumnName = columnName.name
            val valuesToAdd   = values.toList
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $rawColumnName + $parameter"
            val column        = BindMarker.make(columnName, valuesToAdd)(ev)
            (queryString, BindMarkers.empty + column)

          case AssignValueAtListIndex(columnName, index, value, ev) =>
            // example_column[:example_column_index] = :example_column_index
            val rawColumnName = columnName.name

            val indexName      = s"${rawColumnName}_$index"
            val indexParameter = s":$indexName"

            val valueName      = s"${rawColumnName}_${index}_value"
            val valueParameter = s":$valueName"

            val queryString = s"$rawColumnName[$indexParameter] = $valueParameter"
            val indexColumn = BindMarker.make(BindMarkerName.make(indexName), index)(CqlRowComponentEncoder[Int])
            val valueColumn = BindMarker.make(BindMarkerName.make(valueName), value)(ev)
            (queryString, BindMarkers.empty + indexColumn + valueColumn)

          case AddSetItems(columnName, value, ev) =>
            // example_column = example_column + :example_column
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $rawColumnName + $parameter"
            val column        = BindMarker.make(columnName, value.toSet)(ev)
            (queryString, BindMarkers.empty + column)

          case RemoveSetItems(columnName, value, ev) =>
            // example_column = example_column - :example_column
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $rawColumnName - $parameter"
            val column        = BindMarker.make(columnName, value.toSet)(ev)
            (queryString, BindMarkers.empty + column)

          case AppendMapItems(columnName, entries, ev) =>
            // example_column = example_column + :example_column
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $rawColumnName + $parameter"
            val column        = BindMarker.make(columnName, entries.toMap)(ev)
            (queryString, BindMarkers.empty + column)

          case RemoveMapItemsByKey(columnName, keys, evK) =>
            // example_column = example_column - :example_column
            val rawColumnName = columnName.name
            val parameter     = s":$rawColumnName"
            val queryString   = s"$rawColumnName = $rawColumnName - $parameter"
            val column        = BindMarker.make(columnName, keys.toList)(evK)
            (queryString, BindMarkers.empty + column)

          case AssignValueAtMapKey(columnName, key, value, evK, evV) =>
            // example_column[:example_column_key] = :example_column_key_value
            val rawColumnName = columnName.name

            val keyName      = s"${rawColumnName}_$key"
            val keyParameter = s":$keyName"

            val valueName      = s"${rawColumnName}_${key}_value"
            val valueParameter = s":$valueName"

            val queryString = s"$rawColumnName[$keyParameter] = $valueParameter"
            val keyColumn   = BindMarker.make(BindMarkerName.make(keyName), key)(evK)
            val valueColumn = BindMarker.make(BindMarkerName.make(valueName), value)(evV)
            (queryString, BindMarkers.empty + keyColumn + valueColumn)
        }

      val columns     = renderedAssignments.map(_._2).reduce(_ ++ _)
      val queryString = renderedAssignments.map(_._1).mkString(start = "SET ", sep = ",", end = "")

      (queryString, columns)
    }
  }

  private object delete {
    def render(table: String, relations: NonEmptyChunk[Relation]): (String, BindMarkers) = {
      val (relationsCql, relationsColumns) = renderRelations(relations)
      (s"DELETE FROM $table WHERE $relationsCql", relationsColumns)
    }
  }

  private object truncate {
    def render(table: String): (String, BindMarkers) =
      (s"TRUNCATE $table", BindMarkers.empty)
  }

  private object select {
    def render(
      tableName: String,
      columnNames: NonEmptyChunk[String],
      relations: Chunk[Relation]
    ): (String, BindMarkers) = {
      val (relationsCql, relationBindMarkers) = renderRelations(relations)
      val columnNamesCql                      = columnNames.mkString(start = "", sep = ",", end = "")

      (s"SELECT $columnNamesCql FROM $tableName $relationsCql", relationBindMarkers)
    }
  }

  /**
   * renderRelations renders a list of relations into a string along with the
   * data that needs to be inserted into the driver's statement
   *
   * For example Chunk("a" > 1, "b" === 2, "c" < 3) will become "WHERE a >
   * :a_relation AND b = :b_relation AND c < :c_relation" along with
   * Columns(a_relation -> ..., b_relation -> ..., c_relation -> ...)
   *
   * @param relations
   * @return
   */
  private def renderRelations(relations: Chunk[Relation]): (String, BindMarkers) =
    if (relations.isEmpty) ("", BindMarkers.empty)
    else {
      val initial = (Chunk[String](), BindMarkers.empty)
      val (exprChunk, columns) =
        relations.foldLeft(initial) { case ((accExpr, accColumns), relation) =>
          relation match {
            case Relation.Binary(columnName, operator, value, encoder) =>
              // For example, where("col1" >= 1) becomes
              // "col1 >= :col1_relation" along with Columns("col1_relation" -> 1 with write capabilities)
              val param      = s"${columnName.name}_relation"
              val column     = BindMarker.make(BindMarkerName.make(param), value)(encoder)
              val expression = s"${columnName.name} ${operator.render} :$param"
              (accExpr :+ expression, accColumns + column)

            case Relation.IsNotNull(columnName) =>
              val expression = s"${columnName.name} IS NOT NULL"
              (accExpr :+ expression, accColumns)

            case Relation.IsNull(columnName) =>
              val expression = s"${columnName.name} == NULL"
              (accExpr :+ expression, accColumns)
          }
        }
      val relationExpr = "WHERE " ++ exprChunk.mkString(" AND ")
      (relationExpr, columns)
    }
}
