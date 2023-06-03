package io.kaizensolutions.virgil

import org.scalacheck.Gen

package object models {
  implicit class ScalaCheckGenOps(val self: Gen.type) {
    def int: Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)

    def map[K, V](key: Gen[K], value: Gen[V]): Gen[Map[K, V]] =
      Gen.mapOf(Gen.zip(key, value))

    def nonEmptyMap[K, V](key: Gen[K], value: Gen[V]): Gen[Map[K, V]] =
      Gen.nonEmptyMap(Gen.zip(key, value))

    def listOfBounded[A](min: Int, max: Int)(elem: Gen[A]): Gen[List[A]] =
      for {
        n      <- Gen.chooseNum(min, max)
        result <- Gen.listOfN(n, elem)
        if result.length >= min
      } yield result

    def setOf[A](element: Gen[A]): Gen[Set[A]] =
      Gen.listOf(element).map(_.toSet)

    def nonEmptySetOf[A](element: Gen[A]): Gen[Set[A]] =
      Gen.nonEmptyListOf(element).map(_.toSet)

    def vectorOf[A](element: Gen[A]): Gen[Vector[A]] =
      Gen.listOf(element).map(_.toVector)

    def nonEmptyVectorOf[A](element: Gen[A]): Gen[Vector[A]] =
      Gen.nonEmptyListOf(element).map(_.toVector)

    def stringBounded(min: Int, max: Int)(elem: Gen[Char]): Gen[String] =
      for {
        n <- Gen.chooseNum(min, max)
        s <- Gen.stringOfN(n, elem)
        if s.length >= min
      } yield s
  }
}
