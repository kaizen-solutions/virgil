package io.kaizensolutions.virgil.internal

import io.kaizensolutions.virgil.CQLType
import io.kaizensolutions.virgil.CQLType.Mutation
import io.kaizensolutions.virgil.CQLType.Mutation.Delete.DeleteCriteria
import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.dsl.{
  Assignment,
  Conditions,
  DeleteConditions,
  InsertConditions,
  Relation,
  UpdateConditions
}
import zio._

private[virgil] object CqlStatementRenderer {
  def render(in: CQLType.Mutation): (String, BindMarkers) =
    in match {
      case Mutation.Insert(tableName, columns, conditions, timeToLive, timestamp) =>
        insert.render(tableName, columns, conditions, timeToLive, timestamp)

      case Mutation.Update(tableName, assignments, relations, conditions) =>
        update.render(tableName, assignments, relations, conditions)

      case Mutation.Delete(tableName, criteria, relations, conditions) =>
        delete.render(tableName, criteria, relations, conditions)

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
    def render(
      table: String,
      columns: BindMarkers,
      conditions: InsertConditions,
      timeToLive: Option[Duration],
      timestamp: Option[Long]
    ): (String, BindMarkers) = {
      val (columnNamesCql, bindMarkersCql) = {
        val size          = columns.underlying.size
        val columnBuilder = ChunkBuilder.make[String](size)
        val markerBuilder = ChunkBuilder.make[String](size)

        columns.underlying.keysIterator.foreach { next =>
          val columnName = next.name
          val marker     = s":$columnName"
          columnBuilder += columnName
          markerBuilder += marker
        }

        val renderedColumnNames = columnBuilder.result().mkString(start = "(", sep = ", ", end = ")")
        val renderedMarkers     = markerBuilder.result().mkString(start = "(", sep = ", ", end = ")")
        (renderedColumnNames, renderedMarkers)
      }

      val (conditionsCql, conditionColumns) = renderConditions(conditions)
      val ttlTimestampCql                   = renderUsingTtlAndTimestamp(timeToLive, timestamp)

      val allColumns = columns ++ conditionColumns
      val insertCql  = s"INSERT INTO $table $columnNamesCql VALUES $bindMarkersCql $conditionsCql $ttlTimestampCql".trim

      (insertCql, allColumns)
    }
  }

  private object update {
    def render(
      table: String,
      assignments: NonEmptyChunk[Assignment],
      relations: NonEmptyChunk[Relation],
      conditions: UpdateConditions
    ): (String, BindMarkers) = {
      val (assignmentCql, assignmentColumns) = renderAssignments(assignments)
      val (relationsCql, relationsColumns)   = renderWhere(relations)
      val (conditionsCql, conditionsColumns) = renderConditions(conditions)
      val allColumns                         = assignmentColumns ++ relationsColumns ++ conditionsColumns

      (s"UPDATE $table $assignmentCql $relationsCql $conditionsCql", allColumns)
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
            val queryString   = s"$rawColumnName = $rawColumnName $sign $parameter"
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
    def render(
      table: String,
      criteria: DeleteCriteria,
      relations: NonEmptyChunk[Relation],
      conditions: DeleteConditions
    ): (String, BindMarkers) = {
      val criteriaCql                        = renderCriteria(criteria)
      val (whereCql, relationsColumns)       = renderWhere(relations)
      val (conditionsCql, conditionsColumns) = renderConditions(conditions)
      val allColumns                         = relationsColumns ++ conditionsColumns

      (s"DELETE $criteriaCql FROM $table $whereCql $conditionsCql", allColumns)
    }

    private def renderCriteria(in: DeleteCriteria): String = in match {
      case DeleteCriteria.Columns(columnNames) => columnNames.mkString(", ")
      case DeleteCriteria.EntireRow            => ""
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
      val (relationsCql, relationBindMarkers) = renderWhere(relations)
      val columnNamesCql                      = columnNames.mkString(start = "", sep = ", ", end = "")

      (s"SELECT $columnNamesCql FROM $tableName $relationsCql", relationBindMarkers)
    }
  }

  private def renderUsingTtlAndTimestamp(timeToLive: Option[Duration], timestamp: Option[Long]): String = {
    val renderedTTL       = timeToLive.map(d => s"TTL ${d.getSeconds}")
    val renderedTimestamp = timestamp.map(t => s"TIMESTAMP $t")

    (renderedTTL, renderedTimestamp) match {
      case (Some(ttl), Some(ts)) => s"USING $ttl AND $ts"
      case (Some(ttl), None)     => s"USING $ttl"
      case (None, Some(ts))      => s"USING $ts"
      case (None, None)          => ""
    }
  }

  private def renderWhere(relations: Chunk[Relation]): (String, BindMarkers) =
    renderRelations("WHERE", relations)

  private def renderConditions(conditions: Conditions): (String, BindMarkers) =
    conditions match {
      case Conditions.NoConditions =>
        ("", BindMarkers.empty)

      case Conditions.IfExists =>
        ("IF EXISTS", BindMarkers.empty)

      case Conditions.IfNotExists =>
        ("IF NOT EXISTS", BindMarkers.empty)

      case Conditions.IfConditions(conditions) =>
        renderRelations("IF", conditions)
    }

  /**
   * renderRelations renders a list of relations into a string along with the
   * data that needs to be inserted into the driver's statement
   *
   * For example Chunk("a" > 1, "b" === 2, "c" < 3) will become "YourPrefix a >
   * :a_relation AND b = :b_relation AND c < :c_relation" along with
   * Columns(a_relation -> ..., b_relation -> ..., c_relation -> ...)
   *
   * @param relations
   * @return
   */
  private def renderRelations(prefix: String, relations: Chunk[Relation]): (String, BindMarkers) =
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
              val expression = s"${columnName.name} != NULL"
              (accExpr :+ expression, accColumns)

            case Relation.IsNull(columnName) =>
              val expression = s"${columnName.name} = NULL"
              (accExpr :+ expression, accColumns)
          }
        }
      val relationExpr = s"$prefix " ++ exprChunk.mkString(" AND ")
      (relationExpr, columns)
    }
}
