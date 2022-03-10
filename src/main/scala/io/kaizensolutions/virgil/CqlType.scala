package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.dsl.{Assignment, Relation}
import io.kaizensolutions.virgil.internal.{BindMarkers, CqlStatementRenderer, PullMode, QueryType}
import zio.NonEmptyChunk

sealed trait CQLType[+Result] { self =>
  def debug: String = {
    def renderSingle(queryString: String, markers: BindMarkers): String =
      s"$queryString ${System.lineSeparator()} - $markers"

    self match {
      case mutation: CQLType.Mutation =>
        val (queryString, bindMarkers) = CqlStatementRenderer.render(mutation)
        renderSingle(queryString, bindMarkers)

      case b: CQLType.Batch =>
        val batchRendered = b.mutations.map(_.debug)
        batchRendered.mkString(
          start = "BATCH(" + System.lineSeparator(),
          sep = ", " + System.lineSeparator(),
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
    final private[virgil] case class Insert(tableName: String, data: BindMarkers) extends Mutation

    final private[virgil] case class Update(
      tableName: String,
      assignments: NonEmptyChunk[Assignment],
      relations: NonEmptyChunk[Relation]
    ) extends Mutation

    final private[virgil] case class Delete(
      tableName: String,
      relations: NonEmptyChunk[Relation]
    ) extends Mutation

    final private[virgil] case class Truncate(tableName: String) extends Mutation

    final private[virgil] case class RawCql private (queryString: String, bindMarkers: BindMarkers) extends Mutation
  }

  final private[virgil] case class Batch(mutations: NonEmptyChunk[Mutation], batchType: BatchType)
      extends CQLType[MutationResult]

  final private[virgil] case class Query[Result](
    queryType: QueryType,
    reader: CqlRowDecoder.Object[Result],
    pullMode: PullMode
  ) extends CQLType[Result]
}
