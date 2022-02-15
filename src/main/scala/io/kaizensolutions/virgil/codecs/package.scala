package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue

package object codecs {
  type RowReader[A] = Reader.WithDriver[A, Row]
  type UdtReader[A] = Reader.WithDriver[A, UdtValue]
  type UdtWriter[A] = Writer.WithDriver[A, UdtValue]
}
