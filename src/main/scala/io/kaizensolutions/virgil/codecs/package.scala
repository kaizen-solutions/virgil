package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue

package object codecs {
  type CqlDecoder[A] = CqlColumnDecoder.WithDriver[A, Row]
  type UdtDecoder[A] = CqlColumnDecoder.WithDriver[A, UdtValue]
  type UdtEncoder[A] = CqlColumnEncoder.WithDriver[A, UdtValue]
}
