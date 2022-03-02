package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.data.CqlDuration
import zio.schema.Schema

package object codecs {
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

}
