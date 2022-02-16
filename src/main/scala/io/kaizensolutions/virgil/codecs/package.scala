package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue

package object codecs {
  type Decoder[A]    = ColumnDecoder.WithDriver[A, Row]
  type UdtDecoder[A] = ColumnDecoder.WithDriver[A, UdtValue]
  type UdtEncoder[A] = ColumnEncoder.WithDriver[A, UdtValue]
}
