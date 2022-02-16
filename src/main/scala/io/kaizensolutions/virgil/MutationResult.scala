package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.SettableByName
import io.kaizensolutions.virgil.codecs.{Reader, Writer}

final case class MutationResult private (result: Boolean) extends AnyVal
object MutationResult {
  def make(result: Boolean): MutationResult = MutationResult(result)

  // Make it such that you cannot accidentally create a Query of a MutationResult because this is an invalid state
  implicit val writerAmbiguous1: Writer[MutationResult] =
    new Writer[MutationResult] {
      override type DriverType = MutationResult

      override def driverClass: Class[MutationResult] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: MutationResult, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: MutationResult,
        structure: Structure
      ): Structure = structure
    }

  implicit val writerAmbiguous2: Writer[MutationResult] =
    new Writer[MutationResult] {
      override type DriverType = MutationResult

      override def driverClass: Class[MutationResult] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: MutationResult, dataType: DataType): DriverType = scalaValue

      override def write[Structure <: SettableByName[Structure]](
        key: String,
        value: MutationResult,
        structure: Structure
      ): Structure = structure
    }

  implicit val readerAmbiguous1: Reader[MutationResult] =
    Reader.make(classOf[MutationResult])(identity)((_, _) => MutationResult(true))

  implicit val readerAmbiguous2: Reader[MutationResult] =
    Reader.make(classOf[MutationResult])(identity)((_, _) => MutationResult(true))
}
