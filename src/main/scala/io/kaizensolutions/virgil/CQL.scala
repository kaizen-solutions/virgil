package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.Reader
import io.kaizensolutions.virgil.configuration.ExecutionAttributes
import io.kaizensolutions.virgil.dsl.{Assignment, Relation}
import zio._

final case class CQL[+Result] private (
  private[virgil] val cqlType: CQLType[Result],
  private[virgil] val executionAttributes: ExecutionAttributes
) { self =>
  def +(that: CQL[MutationResult])(implicit ev: Result <:< MutationResult): CQL[MutationResult] = {
    val resultCqlType =
      (self.widen[MutationResult].cqlType, that.cqlType) match {
        case (a: CQLType.Mutation, b: CQLType.Mutation) =>
          CQLType.Batch(NonEmptyChunk(a, b), BatchType.Logged)

        case (a: CQLType.Mutation, CQLType.Batch(mutations, thatBatchType)) =>
          CQLType.Batch(a +: mutations, thatBatchType)

        case (CQLType.Batch(mutations, batchType), b: CQLType.Mutation) =>
          CQLType.Batch(mutations :+ b, batchType)

        case (CQLType.Batch(m1, b1), CQLType.Batch(m2, b2)) =>
          CQLType.Batch(m1 ++ m2, b1.combine(b2))

        case (b @ CQLType.Batch(_, _), _) =>
          b

        case (_, b @ CQLType.Batch(_, _)) =>
          b

        // You cannot actually construct a Query[MutationResult]
        case (m: CQLType.Mutation, _) =>
          m

        case (_, m: CQLType.Mutation) =>
          m

        case (a, _) =>
          a
      }
    CQL(resultCqlType, self.executionAttributes.combine(that.executionAttributes))
  }

  def batchType(in: BatchType)(implicit ev: Result <:< MutationResult): CQL[MutationResult] =
    self.cqlType match {
      case _: CQLType.Mutation =>
        self.widen[MutationResult]

      case b: CQLType.Batch =>
        self.copy(cqlType = b.copy(batchType = in))

      case CQLType.Query(_, _, _) =>
        self.widen[MutationResult] // Technically this is not possible due to type constraints
    }

  def take[Result1 >: Result](n: Int)(implicit ev: CQLType[Result1] <:< CQLType.Query[Result1]): CQL[Result1] = {
    val adjustN = n match {
      case invalid if invalid <= 0 => 1
      case _                       => n
    }
    val query = ev(cqlType)
    copy(cqlType = CQLType.Query(query.queryType, query.reader, PullMode.TakeUpto(adjustN)))
  }

  def all[Result1 >: Result](implicit ev: CQLType[Result1] <:< CQLType.Query[Result1]): CQL[Result1] =
    copy(cqlType = ev(cqlType).copy(pullMode = PullMode.All))

  def withAttributes(in: ExecutionAttributes): CQL[Result] =
    copy(executionAttributes = in)

  private def widen[AnotherResult](implicit ev: Result <:< AnotherResult): CQL[AnotherResult] = {
    val _                    = ev // Can make use of liftCo in 2.13.x but not in 2.12.x :(
    val anotherResultCqlType = self.cqlType.asInstanceOf[CQLType[AnotherResult]]
    self.copy(cqlType = anotherResultCqlType)
  }
}
object CQL {
  def batch(in: CQL[MutationResult], batchType: BatchType = BatchType.Logged): CQL[MutationResult] =
    in.cqlType match {
      case mutation: CQLType.Mutation  => CQL(CQLType.Batch(NonEmptyChunk(mutation), batchType), in.executionAttributes)
      case CQLType.Batch(mutations, _) => in.copy(cqlType = CQLType.Batch(mutations, batchType))
      case CQLType.Query(_, _, _)      => in
    }

  def cqlMutation(queryString: String, bindMarkers: BindMarkers): CQL[MutationResult] =
    CQL(CQLType.Mutation.RawCql(queryString, bindMarkers), ExecutionAttributes.default)

  def cqlQuery[Scala](queryString: String, bindMarkers: BindMarkers, pullMode: PullMode = PullMode.All)(implicit
    reader: Reader[Scala]
  ): CQL[Scala] =
    CQL(CQLType.Query(QueryType.RawCql(queryString, bindMarkers), reader, pullMode), ExecutionAttributes.default)

  def delete(tableName: String, relations: NonEmptyChunk[Relation]): CQL[MutationResult] =
    CQL(CQLType.Mutation.Delete(tableName, relations), ExecutionAttributes.default)

  def insert(
    tableName: String,
    data: BindMarkers
  ): CQL[MutationResult] =
    CQL(CQLType.Mutation.Insert(tableName, data), ExecutionAttributes.default)

  def select[Scala](
    tableName: String,
    columns: NonEmptyChunk[String],
    relations: Chunk[Relation],
    pullMode: PullMode = PullMode.All
  )(implicit
    reader: Reader[Scala]
  ): CQL[Scala] =
    CQL(
      CQLType
        .Query(QueryType.Select(tableName = tableName, columnNames = columns, relations = relations), reader, pullMode),
      ExecutionAttributes.default
    )

  def truncate(tableName: String): CQL[MutationResult] =
    CQL(CQLType.Mutation.Truncate(tableName), ExecutionAttributes.default)

  def update(
    tableName: String,
    assignments: NonEmptyChunk[Assignment],
    relations: NonEmptyChunk[Relation]
  ): CQL[MutationResult] =
    CQL(CQLType.Mutation.Update(tableName, assignments, relations), ExecutionAttributes.default)
}
