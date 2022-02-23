//package io.kaizensolutions.virgil.codecs
//
//import com.datastax.oss.driver.api.core.cql.Row
//import scala.util.Try
//
//trait CqlDecoder[+ScalaType] { self =>
//  def decode(structure: Row): Either[String, ScalaType]
//
//  def map[ScalaType2](f: ScalaType => ScalaType2): CqlDecoder[ScalaType2] = new CqlDecoder[ScalaType2] {
//    override def decode(structure: Row): Either[String, ScalaType2] = self.decode(structure).map(f)
//  }
//
//  // flesh out more combinators (like zip, etc.)
//}
//
//object CqlDecoder {
//  implicit def cqlDecoderForTuple2[A, B](implicit
//    columnDecoderForA: CqlColumnDecoder[A],
//    columnDecoder: CqlColumnDecoder[B]
//  ): CqlDecoder[(A, B)] =
//    new CqlDecoder[(A, B)] {
//      override def decode(structure: Row): Either[String, (A, B)] = {
//        val a = Try(columnDecoderForA.decodeFieldByIndex(structure, 0)).toEither.left.map(_.getMessage)
//        val b = Try(columnDecoder.decodeFieldByIndex(structure, 1)).toEither.left.map(_.getMessage)
//        a.flatMap(a => b.map(b => (a, b)))
//      }
//    }
//}
//
//case class Address(street: String, city: String)
//case class Person(id: String, name: String, age: Int, address: Address)
//object Person {
//  val cqlDecoderForPerson: CqlDecoder[Person] = new CqlDecoder[Person] {
//    override def decode(structure: Row): Either[String, Person] = {
//      val id   = Try(CqlColumnDecoder[String].decodeFieldByName(structure, "id")).toEither.left.map(_.getMessage)
//      val name = Try(CqlColumnDecoder[String].decodeFieldByName(structure, "name")).toEither.left.map(_.getMessage)
//      val age  = Try(CqlColumnDecoder[Int].decodeFieldByName(structure, "age")).toEither.left.map(_.getMessage)
//      val address =
//        Try(CqlColumnDecoder[Address].decodeFieldByName(structure, "address")).toEither.left.map(_.getMessage)
//
//      for {
//        id   <- id
//        name <- name
//        age  <- age
//        addr <- address
//      } yield Person(id, name, age, addr)
//    }
//  }
//}
