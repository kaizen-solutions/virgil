package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.CQLType.Mutation.Delete
import io.kaizensolutions.virgil.codecs.CqlRowDecoder
import io.kaizensolutions.virgil.configuration.ExecutionAttributes
import io.kaizensolutions.virgil.dsl.Assignment
import io.kaizensolutions.virgil.dsl.DeleteConditions
import io.kaizensolutions.virgil.dsl.InsertConditions
import io.kaizensolutions.virgil.dsl.Relation
import io.kaizensolutions.virgil.dsl.UpdateConditions
import io.kaizensolutions.virgil.internal.BindMarkers
import io.kaizensolutions.virgil.internal.Proofs._
import io.kaizensolutions.virgil.internal.PullMode
import io.kaizensolutions.virgil.internal.QueryType

import java.time.Duration

final case class CQL[+Result](
  private[virgil] val cqlType: CQLType[Result],
  private[virgil] val executionAttributes: ExecutionAttributes
) { self =>
  def +(that: CQL[MutationResult])(implicit ev: Result <:< MutationResult): CQL[MutationResult] = {
    val resultCqlType =
      (self.widen[MutationResult].cqlType, that.cqlType) match {
        case (a: CQLType.Mutation, b: CQLType.Mutation) =>
          CQLType.Batch(IndexedSeq(a, b), BatchType.Logged)

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

  def debug: String =
    s"Query: ${cqlType.debug}" + java.lang.System.lineSeparator() + s" - ${executionAttributes.debug}"

  def pageSize(in: Int): CQL[Result] =
    if (in > 0) self.withAttributes(self.executionAttributes.copy(pageSize = Option(in)))
    else self

  def take[Result1 >: Result](n: Long)(implicit ev: Result1 <:!< MutationResult): CQL[Result1] = {
    val _       = ev
    val adjustN = n match {
      case invalid if invalid <= 0 => 1
      case _                       => n
    }
    cqlType match {
      case q @ CQLType.Query(_, _, _) => self.copy(cqlType = q.copy(pullMode = PullMode.TakeUpto(adjustN)))
      // The following matches are not possible due to type constraints expressed above
      case _: CQLType.Mutation => sys.error("It is not possible to take a mutation")
      case _: CQLType.Batch    => sys.error("It is not possible to take a batch")
    }
  }

  def timeout(in: Duration): CQL[Result] =
    self.withAttributes(self.executionAttributes.withTimeout(in))

  def withAttributes(in: ExecutionAttributes): CQL[Result] =
    copy(executionAttributes = in)

  private[virgil] def widen[AnotherResult](implicit ev: Result <:< AnotherResult): CQL[AnotherResult] = {
    val _                    = ev // Can make use of liftCo in 2.13.x but not in 2.12.x :(
    val anotherResultCqlType = self.cqlType.asInstanceOf[CQLType[AnotherResult]]
    self.copy(cqlType = anotherResultCqlType)
  }
}
object CQL {
  def batch(in: CQL[MutationResult], batchType: BatchType = BatchType.Logged): CQL[MutationResult] =
    in.cqlType match {
      case mutation: CQLType.Mutation  => CQL(CQLType.Batch(IndexedSeq(mutation), batchType), in.executionAttributes)
      case CQLType.Batch(mutations, _) => in.copy(cqlType = CQLType.Batch(mutations, batchType))
      case CQLType.Query(_, _, _)      => in
    }

  def cqlMutation(queryString: String, bindMarkers: BindMarkers): CQL[MutationResult] =
    CQL(CQLType.Mutation.RawCql(queryString, bindMarkers), ExecutionAttributes.default)

  def cqlQuery[Scala](queryString: String, bindMarkers: BindMarkers, pullMode: PullMode = PullMode.All)(implicit
    reader: CqlRowDecoder.Object[Scala]
  ): CQL[Scala] =
    CQL(CQLType.Query(QueryType.RawCql(queryString, bindMarkers), reader, pullMode), ExecutionAttributes.default)

  def delete(
    tableName: String,
    criteria: Delete.DeleteCriteria,
    relations: IndexedSeq[Relation],
    conditions: DeleteConditions
  ): CQL[MutationResult] =
    CQL(
      cqlType = CQLType.Mutation.Delete(
        tableName = tableName,
        criteria = criteria,
        relations = relations,
        deleteConditions = conditions
      ),
      executionAttributes = ExecutionAttributes.default
    )

  def insert(
    tableName: String,
    data: BindMarkers,
    conditions: InsertConditions,
    timeToLive: Option[Duration],
    timestamp: Option[Long]
  ): CQL[MutationResult] =
    CQL(CQLType.Mutation.Insert(tableName, data, conditions, timeToLive, timestamp), ExecutionAttributes.default)

  def logged(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Logged)

  def unlogged(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Unlogged)

  def counter(cql: CQL[MutationResult]): CQL[MutationResult] = cql.batchType(BatchType.Counter)

  def select[Scala](
    tableName: String,
    columns: IndexedSeq[String],
    relations: IndexedSeq[Relation]
  )(implicit
    reader: CqlRowDecoder.Object[Scala]
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
    assignments: IndexedSeq[Assignment],
    relations: IndexedSeq[Relation],
    updateConditions: UpdateConditions
  ): CQL[MutationResult] =
    CQL(
      CQLType.Mutation.Update(
        tableName = tableName,
        assignments = assignments,
        relations = relations,
        updateConditions = updateConditions
      ),
      executionAttributes = ExecutionAttributes.default
    )

  implicit class CQLQueryOps[A](in: CQL[A])(implicit ev: A <:!< MutationResult) {
    locally {
      val _ = ev
    }
    def readAs[B](implicit reader: CqlRowDecoder.Object[B]): CQL[B] =
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
