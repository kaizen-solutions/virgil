package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.codecs.RowReader
import io.kaizensolutions.virgil.configuration.{ExecutionAttributes, PageState}
import io.kaizensolutions.virgil.dsl.{Assignment, Relation}
import io.kaizensolutions.virgil.internal.Proofs._
import io.kaizensolutions.virgil.internal.{BindMarkers, PullMode, QueryType}
import zio._
import zio.stream.ZStream

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

  def all[Result1 >: Result](implicit ev: CQLType[Result1] <:< CQLType.Query[Result1]): CQL[Result1] =
    copy(cqlType = ev(cqlType).copy(pullMode = PullMode.All))

  def batchType(in: BatchType)(implicit ev: Result <:< MutationResult): CQL[MutationResult] =
    self.cqlType match {
      case _: CQLType.Mutation =>
        self.widen[MutationResult]

      case b: CQLType.Batch =>
        self.copy(cqlType = b.copy(batchType = in))

      case CQLType.Query(_, _, _) =>
        self.widen[MutationResult] // Technically this is not possible due to type constraints
    }

  def execute: ZStream[Has[CQLExecutor], Throwable, Result] = CQLExecutor.execute(self)

  def executePage[Result1 >: Result](state: Option[PageState] = None): RIO[Has[CQLExecutor], Paged[Result1]] =
    CQLExecutor.executePage(self, state)

  def take[Result1 >: Result](n: Long)(implicit ev: CQLType[Result1] <:< CQLType.Query[Result1]): CQL[Result1] = {
    val adjustN = n match {
      case invalid if invalid <= 0 => 1
      case _                       => n
    }
    val query = ev(cqlType)
    copy(cqlType = CQLType.Query(query.queryType, query.reader, PullMode.TakeUpto(adjustN)))
  }

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
    reader: RowReader[Scala]
  ): CQL[Scala] =
    CQL(CQLType.Query(QueryType.RawCql(queryString, bindMarkers), reader, pullMode), ExecutionAttributes.default)

  def delete(tableName: String, relations: NonEmptyChunk[Relation]): CQL[MutationResult] =
    CQL(CQLType.Mutation.Delete(tableName, relations), ExecutionAttributes.default)

  def insert(
    tableName: String,
    data: BindMarkers
  ): CQL[MutationResult] =
    CQL(CQLType.Mutation.Insert(tableName, data), ExecutionAttributes.default)

  def logged(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Logged)

  def unlogged(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Unlogged)

  def counter(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Counter)

  def select[Scala](
    tableName: String,
    columns: NonEmptyChunk[String],
    relations: Chunk[Relation]
  )(implicit
    reader: RowReader[Scala]
  ): CQL[Scala] =
    CQL(
      CQLType
        .Query(
          queryType = QueryType.Select(tableName = tableName, columnNames = columns, relations = relations),
          reader = reader,
          pullMode = PullMode.All
        ),
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

  implicit class CQLQueryOps[A](in: CQL[A])(implicit ev: A <:!< MutationResult) {
    def readAs[B](implicit reader: RowReader[B]): CQL[B] =
      in.cqlType match {
        case CQLType.Query(queryType, _, pullMode) =>
          CQL(CQLType.Query(queryType, reader, pullMode), in.executionAttributes)

        // These are not possible due to type constraints
        case _: CQLType.Mutation =>
          sys.error("It is not possible to change a mutation to a query")

        case _: CQLType.Batch =>
          sys.error("It is not possible to change a batch mutation to a query")
      }
  }
}
