package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.`type`.codec.CodecNotFoundException
import com.datastax.oss.driver.internal.core.`type`.UserDefinedTypeBuilder
import io.kaizensolutions.virgil.codecs.CqlPrimitiveDecoder
import zio.ZIO
import zio.test.Assertion._
import zio.test._

object CodecSpec extends ZIOSpecDefault {
  def spec: Spec[Any, Nothing] = suite("Codec Specification") {
    suite("Primitive Codecs") {
      val userDefinedType: UserDefinedType =
        new UserDefinedTypeBuilder("internal_keyspace", "either_test")
          .withField("a", DataTypes.TEXT)
          .build()

      suite("Either") {
        test("Reading the incorrect type returns a Left") {
          val udt        = userDefinedType.newValue().setString("a", "foo")
          val intDecoder = CqlPrimitiveDecoder[Int]
          val result     = CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(intDecoder.either)
          assert(result.left.map(_.debug))(isLeft(containsString("Failed to read a")))
        } +
          test("Reading the correct type but encountering a null value returns a Left") {
            val udt           = userDefinedType.newValue().setToNull("a")
            val stringDecoder = CqlPrimitiveDecoder[String]
            val result        = CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(stringDecoder.either)
            assert(result.left.map(_.debug))(isLeft(containsString("not an optional field")))
          } +
          test("Reading the correct type returns a Right") {
            val udt           = userDefinedType.newValue().setString("a", "yay")
            val stringDecoder = CqlPrimitiveDecoder[String]
            val result        = CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(stringDecoder.either)
            assert(result)(isRight(equalTo("yay")))
          }
      } +
        suite("Option") {
          test("Reading a null value returns None") {
            val udt           = userDefinedType.newValue().setToNull("a")
            val stringDecoder = CqlPrimitiveDecoder[String]
            val result        = CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(stringDecoder.optional)
            assert(result)(isNone)
          } +
            test("Reading a value returns Some") {
              val udt           = userDefinedType.newValue().setString("a", "yay")
              val stringDecoder = CqlPrimitiveDecoder[String]
              val result        = CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(stringDecoder.optional)
              assert(result)(isSome(containsString("yay")))
            } +
            test("Reading an incorrect type will throw an exception") {
              val udt        = userDefinedType.newValue().setString("a", "nay")
              val intDecoder = CqlPrimitiveDecoder[Int]
              ZIO
                .attempt(CqlPrimitiveDecoder.decodePrimitiveByFieldName(udt, "a")(intDecoder.optional))
                .refineToOrDie[CodecNotFoundException]
                .exit
                .map(result => assert(result)(failsWithA[CodecNotFoundException]))
            }
        }
    }
  }
}
