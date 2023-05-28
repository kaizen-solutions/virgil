package io.kaizensolutions.virgil

import zio.test._
import zio.test.Assertion._

object TupleCodecSpec extends ZIOSpecDefault {
  def spec: Spec[Any, Nothing] =
    suite("Tuple Codecs specification")(tupleDecodeSpec)

  private def tupleDecodeSpec =
    suite("Tuple Decoder specification") {
      test("Row Decoders for tuples compile") {
        assertZIO(typeCheck {
          """
          import io.kaizensolutions.virgil.codecs.CqlRowDecoder

          CqlRowDecoder[Tuple1[String]]
          CqlRowDecoder[(String, Int)]
          CqlRowDecoder[(String, Int, Float)]
          CqlRowDecoder[(String, Int, Float, Double)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress)]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal], Option[BigInt])]
          CqlRowDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal], Option[BigInt], Option[java.util.UUID])]
          """
        })(isRight(anything))
      } +
        test("UdtValue Decoders for tuples compile") {
          assertZIO(typeCheck {
            """
          import io.kaizensolutions.virgil.codecs.CqlUdtValueDecoder

          CqlUdtValueDecoder[Tuple1[String]]
          CqlUdtValueDecoder[(String, Int)]
          CqlUdtValueDecoder[(String, Int, Float)]
          CqlUdtValueDecoder[(String, Int, Float, Double)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress)]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal], Option[BigInt])]
          CqlUdtValueDecoder[(String, Int, Float, Double, Boolean, Long, BigDecimal, BigInt, java.util.UUID, java.time.Instant, java.net.InetAddress, List[String], Map[String, String], Option[String], Option[Int], Option[Float], Option[Double], Option[Boolean], Option[Long], Option[BigDecimal], Option[BigInt], Option[java.util.UUID])]
          """
          })(isRight(anything))
        }
    }
}
