package io.kaizensolutions.virgil

import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.data.SettableByName
import io.kaizensolutions.virgil.codecs.{CqlColumnDecoder, CqlColumnEncoder}

final case class MutationResult private (result: Boolean) extends AnyVal
object MutationResult {
  def make(result: Boolean): MutationResult = MutationResult(result)

  // Make it such that you cannot accidentally create a Query of a MutationResult because this is an invalid state
  implicit val cqlColumnEncoderForMutationResultAmbiguous1: CqlColumnEncoder[MutationResult] =
    new CqlColumnEncoder[MutationResult] {
      override type DriverType = MutationResult

      override def driverClass: Class[MutationResult] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: MutationResult, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: MutationResult,
        structure: Structure
      ): Structure = structure

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: MutationResult,
        structure: Structure
      ): Structure =
        structure
    }

  implicit val cqlColumnEncoderForMutationResultAmbiguous2: CqlColumnEncoder[MutationResult] =
    new CqlColumnEncoder[MutationResult] {
      override type DriverType = MutationResult

      override def driverClass: Class[MutationResult] = classOf[DriverType]

      override def convertScalaToDriver(scalaValue: MutationResult, dataType: DataType): DriverType = scalaValue

      override def encodeFieldByName[Structure <: SettableByName[Structure]](
        fieldName: String,
        value: MutationResult,
        structure: Structure
      ): Structure = structure

      override def encodeFieldByIndex[Structure <: SettableByName[Structure]](
        index: Int,
        value: MutationResult,
        structure: Structure
      ): Structure = structure
    }

  implicit val cqlColumnDecoderForMutationResultIsAmbiguous1: CqlColumnDecoder[MutationResult] =
    CqlColumnDecoder.make(classOf[MutationResult])(identity)((_, _) => MutationResult(true))((_, _) =>
      MutationResult(true)
    )

  implicit val cqlColumnDecoderForMutationResultIsAmbiguous2: CqlColumnDecoder[MutationResult] =
    CqlColumnDecoder.make(classOf[MutationResult])(identity)((_, _) => MutationResult(true))((_, _) =>
      MutationResult(true)
    )
}
