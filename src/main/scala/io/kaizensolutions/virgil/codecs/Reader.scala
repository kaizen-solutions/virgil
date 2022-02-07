package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.`type`.{ListType, MapType, SetType}
import com.datastax.oss.driver.api.core.cql.{Row => CassandraRow}
import com.datastax.oss.driver.api.core.data.UdtValue
import magnolia1._

import scala.annotation.implicitNotFound
import scala.jdk.CollectionConverters._

/**
 * Reader provides a mechanism to read data from a Cassandra row
 * @tparam ScalaType
 *   to be read from Cassandra
 */
@implicitNotFound(
  """A Reader is not present for ${ScalaType}, if you have a case class, please use Reader.derive[${ScalaType}] 
  to automatically generate a Reader"""
)
trait Reader[ScalaType] { self =>
  def read(columnName: String, row: CassandraRow): ScalaType

  def map[AnotherScalaType](f: ScalaType => AnotherScalaType): Reader[AnotherScalaType] =
    (columnName: String, row: CassandraRow) => f(self.read(columnName, row))

  /**
   * This is mainly used for the Cassandra Row Reader instance so you can have a
   * different Scala representation and not have it map 1:1 with the Cassandra
   * Row. Most likely, the column name fed in will be discarded and unused since
   * the user is interacting with the row directly.
   */
  def zipWith[AnotherScalaType, ResultScalaType](that: Reader[AnotherScalaType])(
    f: (ScalaType, AnotherScalaType) => ResultScalaType
  ): Reader[ResultScalaType] =
    (columnName: String, row: CassandraRow) => f(self.read(columnName, row), that.read(columnName, row))

  def zip[AnotherScalaType](that: Reader[AnotherScalaType]): Reader[(ScalaType, AnotherScalaType)] =
    zipWith(that)((_, _))
}
object Reader extends MagnoliaReaderSupport {
  def apply[A](implicit ev: Reader[A]): Reader[A] = ev

  def make[A](f: (String, CassandraRow) => A): Reader[A] = (columnName: String, row: CassandraRow) => f(columnName, row)

  implicit val bigDecimalReader: Reader[BigDecimal]          = make((columnName, row) => row.getBigDecimal(columnName))
  implicit val bigIntReader: Reader[BigInt]                  = make((columnName, row) => row.getBigInteger(columnName))
  implicit val booleanReader: Reader[Boolean]                = make((columnName, row) => row.getBoolean(columnName))
  implicit val byteBufferReader: Reader[java.nio.ByteBuffer] = make((columnName, row) => row.getByteBuffer(columnName))
  implicit val byteReader: Reader[Byte]                      = make((columnName, row) => row.getByte(columnName))
  implicit val doubleReader: Reader[Double]                  = make((columnName, row) => row.getDouble(columnName))
  implicit val instantReader: Reader[java.time.Instant]      = make((columnName, row) => row.getInstant(columnName))
  implicit val intReader: Reader[Int]                        = make((columnName, row) => row.getInt(columnName))
  implicit val localDateReader: Reader[java.time.LocalDate]  = make((columnName, row) => row.getLocalDate(columnName))
  implicit val localTimeReader: Reader[java.time.LocalTime]  = make((columnName, row) => row.getLocalTime(columnName))
  implicit val longReader: Reader[Long]                      = make((columnName, row) => row.getLong(columnName))
  implicit val shortReader: Reader[Short]                    = make((columnName, row) => row.getShort(columnName))
  implicit val stringReader: Reader[String]                  = make((columnName, row) => row.getString(columnName))
  implicit val udtValueReader: Reader[UdtValue]              = make((columnName, row) => row.getUdtValue(columnName))
  implicit val uuidReader: Reader[java.util.UUID]            = make((columnName, row) => row.getUuid(columnName))

  implicit val cassandraRowReader: Reader[CassandraRow] = make((_, row) => row)

  /**
   * fromRow exposes a low level API to read from a Cassandra row in case you
   * don't want to use derivation. You can use this in addition with zipWith to
   * compose small Readers together to form a larger Reader.
   * @param f
   * @tparam A
   * @return
   */
  def fromRow[A](f: CassandraRow => A): Reader[A] = (_: String, row: CassandraRow) => f(row)

  implicit def optionReader[A](implicit underlying: Reader[A]): Reader[Option[A]] =
    make((columnName, row) => Option(underlying.read(columnName, row)))

  // Handles Sets and nested collections involving Sets
  implicit def deriveSetFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Reader[Set[A]] = {
    (columnName, row) =>
      val datatype     = row.getType(columnName).asInstanceOf[SetType].getElementType
      val cassandraSet = row.getSet(columnName, ev.classType)
      val scala        = cassandraSet.asScala.map(cas => ev.fromCassandra(cas, datatype)).toSet
      scala
  }

  // Handles List and nested collections involving Lists
  implicit def deriveListFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Reader[List[A]] = {
    (columnName, row) =>
      val datatype     = row.getType(columnName).asInstanceOf[ListType].getElementType
      val cassandraSet = row.getList(columnName, ev.classType)
      val scala        = cassandraSet.asScala.map(cas => ev.fromCassandra(cas, datatype)).toList
      scala
  }

  // Handles Maps and nested collections involving Maps
  implicit def deriveMapFromCassandraTypeMapper[K, V](implicit
    evK: CassandraTypeMapper[K],
    evV: CassandraTypeMapper[V]
  ): Reader[Map[K, V]] = { (columnName, row) =>
    val top          = row.getType(columnName).asInstanceOf[MapType]
    val keyType      = top.getKeyType
    val valueType    = top.getValueType
    val cassandraMap = row.getMap(columnName, evK.classType, evV.classType)
    val scala =
      cassandraMap.asScala.map { case (k, v) =>
        (evK.fromCassandra(k, keyType), evV.fromCassandra(v, valueType))
      }.toMap
    scala
  }
}

private[codecs] trait MagnoliaReaderSupport {
  type Typeclass[T] = Reader[T]

  // Only supports case classes since this maps 1:1 with Cassandra's concept of a Row
  def join[T](ctx: CaseClass[Reader, T]): Reader[T] = new Reader[T] {
    override def read(columnName: String, row: CassandraRow): T =
      ctx.construct(param => param.typeclass.read(param.label, row))
  }

  // Semi automatic derivation to avoid conflict with CassandraTypeMapper
  def derive[T]: Reader[T] = macro Magnolia.gen[T]
}
