//package io.kaizensolutions.virgil.codecs
//
//import com.datastax.oss.driver.api.core.`type`.UserDefinedType
//import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder
//import com.datastax.oss.driver.api.core.data.SettableByName
//
///**
// * case class Address(zipCode: String, city: String, street: String) case class
// * Person(id: Int, name: String, age: Int, address: Address)
// *
// * @tparam ScalaType
// */
//
//trait CqlEncoder[-ScalaType] {
//  def encode(value: ScalaType, bsBuilder: BoundStatementBuilder): BoundStatementBuilder
//}
//object CqlEncoder {
//  implicit def cqlEncoderForTuple2[A, B](implicit
//    columnEncoderForA: CqlColumnEncoder[A],
//    columnEncoderForB: CqlColumnEncoder[B]
//  ): CqlEncoder[(A, B)] =
//    new CqlEncoder[(A, B)] {
//      type Structure = SettableByName[BoundStatementBuilder]
//
//      override def encode(value: (A, B), bsBuilder: BoundStatementBuilder): BoundStatementBuilder = {
//        val withA = columnEncoderForA.encodeFieldByIndex(index = 0, value = value._1, structure = bsBuilder)
//        val withB = columnEncoderForB.encodeFieldByIndex(index = 1, value = value._2, structure = withA)
//        withB
//      }
//    }
//
//  implicit def cqlEncoderForTuple3[A, B, C](implicit
//    columnEncoderForA: CqlColumnEncoder[A],
//    columnEncoderForB: CqlColumnEncoder[B],
//    columnEncoderForC: CqlColumnEncoder[C]
//  ): CqlEncoder[(A, B, C)] =
//    new CqlEncoder[(A, B, C)] {
//      type Structure = SettableByName[BoundStatementBuilder]
//
//      override def encode(value: (A, B, C), bsBuilder: BoundStatementBuilder): BoundStatementBuilder = {
//        val withA = columnEncoderForA.encodeFieldByIndex(index = 0, value = value._1, structure = bsBuilder)
//        val withB = columnEncoderForB.encodeFieldByIndex(index = 1, value = value._2, structure = withA)
//        val withC = columnEncoderForC.encodeFieldByIndex(index = 2, value = value._3, structure = withB)
//        withC
//      }
//    }
//}
//
//case class Address2(zipCode: String, city: String)
//case class Person2(id: String, name: String, age: Int, address2: Address2)
//object Person2 {
//  implicit val cqlEncoderForPerson2 = new CqlEncoder[Person2] {
//    override def encode(value: Person2, bsBuilder: BoundStatementBuilder): BoundStatementBuilder = {
//      val withId   = CqlColumnEncoder[String].encodeFieldByName("id", value = value.id, structure = bsBuilder)
//      val withName = CqlColumnEncoder[String].encodeFieldByName("name", value = value.name, structure = withId)
//      val withAge  = CqlColumnEncoder[Int].encodeFieldByName("age", value = value.age, structure = withName)
//      val withAddress = {
//        val addressStructure = bsBuilder.getType("address2").asInstanceOf[UserDefinedType].newValue()
//        val addressPopulated =
//          CqlColumnEncoder[Address2].encodeFieldByName("address2", value = value.address2, structure = addressStructure)
//
//        withAge.setUdtValue("address2", addressPopulated)
//      }
//
//      withAddress
//    }
//  }
//}
