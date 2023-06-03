package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.dsl.Assignment
import io.kaizensolutions.virgil.dsl.DeleteConditions
import io.kaizensolutions.virgil.dsl.InsertConditions
import io.kaizensolutions.virgil.dsl.Relation
import io.kaizensolutions.virgil.dsl.UpdateConditions
import io.kaizensolutions.virgil.internal.BindMarkers
import io.kaizensolutions.virgil.internal.CqlStatementRenderer
import io.kaizensolutions.virgil.internal.PullMode
import io.kaizensolutions.virgil.internal.QueryType

import java.time.Duration

sealed trait CQLType[+Result] { self =>
  def debug: String = {
    def renderSingle(queryString: String, markers: BindMarkers): String =
      s"$queryString ${java.lang.System.lineSeparator()} - $markers"

    self match {
      case mutation: CQLType.Mutation =>
        val (queryString, bindMarkers) = CqlStatementRenderer.render(mutation)
        renderSingle(queryString, bindMarkers)

      case b: CQLType.Batch =>
        val batchRendered = b.mutations.map(_.debug)
        batchRendered.mkString(
          start = "BATCH(" + java.lang.System.lineSeparator(),
          sep = ", " + java.lang.System.lineSeparator(),
          end = s", batch-type = ${b.batchType})"
        )

      case query: CQLType.Query[r] =>
        val (queryString, bindMarkers) = CqlStatementRenderer.render(query)
        renderSingle(queryString, bindMarkers)
    }
  }
}
object CQLType {
  sealed private[virgil] trait Mutation extends CQLType[MutationResult]
  object Mutation {
    final private[virgil] case class Insert(
      tableName: String,
      data: BindMarkers,
      insertConditions: InsertConditions,
      timeToLive: Option[Duration],
      timestamp: Option[Long]
    ) extends Mutation

    final private[virgil] case class Update(
      tableName: String,
      assignments: IndexedSeq[Assignment],
      relations: IndexedSeq[Relation],
      updateConditions: UpdateConditions
    ) extends Mutation

    final private[virgil] case class Delete(
      tableName: String,
      criteria: Delete.DeleteCriteria,
      relations: IndexedSeq[Relation],
      deleteConditions: DeleteConditions
    ) extends Mutation
    object Delete {
      sealed trait DeleteCriteria
      object DeleteCriteria {
        final case class Columns(columnNames: IndexedSeq[String]) extends DeleteCriteria
        case object EntireRow                                     extends DeleteCriteria
      }
    }

    final private[virgil] case class Truncate(tableName: String) extends Mutation

    final private[virgil] case class RawCql(queryString: String, bindMarkers: BindMarkers) extends Mutation
  }

  final private[virgil] case class Batch(mutations: IndexedSeq[Mutation], batchType: BatchType)
      extends CQLType[MutationResult]

  final private[virgil] case class Query[Result](
    queryType: QueryType,
    reader: CqlRowDecoder.Object[Result],
    pullMode: PullMode
  ) extends CQLType[Result]
}
