package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.dsl.{Assignment, Relation}
import io.kaizensolutions.virgil.internal.{BindMarkers, PullMode, QueryType}
import zio.NonEmptyChunk

sealed trait CQLType[+Result]
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

  final private[virgil] case class Query[Result](queryType: QueryType, reader: Reader[Result], pullMode: PullMode)
      extends CQLType[Result]
}
