package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.data.CqlDuration
import zio.Chunk
import zio.schema.{Schema, StandardType}

import scala.util.control.NonFatal

package object codecs extends zio.schema.DefaultJavaTimeSchemas {
  implicit val byteBufferSchema: Schema.Transform[Chunk[Byte], java.nio.ByteBuffer] =
    Schema.Transform(
      codec = Schema.Primitive(StandardType.BinaryType),
      f = bytes => Right(java.nio.ByteBuffer.wrap(bytes.toArray)),
      g = bb => Right(Chunk.fromByteBuffer(bb)),
      Chunk.empty
    )

  implicit val cqlDurationSchema: Schema.CaseClass3[Int, Int, Long, CqlDuration] =
    Schema.CaseClass3(
      Schema.Field("months", Schema.primitive[Int]),
      Schema.Field("days", Schema.primitive[Int]),
      Schema.Field("nanoseconds", Schema.primitive[Long]),
      (m, d, n) => CqlDuration.newInstance(m, d, n),
      _.getMonths,
      _.getDays,
      _.getNanoseconds
    )

  @inline private[virgil] def eitherConvert[A](in: => A): Either[String, A] =
    try Right(in)
    catch { case NonFatal(e) => Left(e.getMessage) }
}
