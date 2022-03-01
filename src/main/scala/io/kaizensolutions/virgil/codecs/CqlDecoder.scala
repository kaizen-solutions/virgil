package io.kaizensolutions.virgil.codecs

import com.datastax.oss.driver.api.core.data.{GettableByName, UdtValue}
import magnolia1._
import zio.Chunk
import zio.schema.Schema

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

// Cannot make covariant due to Magnolia macro
trait CqlDecoder[ScalaType] { self =>
  // Escape Hatch used to connect to CqlColumnDecoder
  protected[virgil] def decodeByName[S <: GettableByName](
    structure: S,
    name: String
  ): Either[List[String], ScalaType] = {
    val _ = name
    decode(structure)
  }

  def decode[S <: GettableByName](structure: S): Either[List[String], ScalaType]

  def map[ScalaType2](f: ScalaType => ScalaType2): CqlDecoder[ScalaType2] = new CqlDecoder[ScalaType2] {
    override def decode[S <: GettableByName](structure: S): Either[List[String], ScalaType2] =
      self.decode(structure).map(f)
  }

  // flesh out more combinators (like zip, etc.)
  def zipWith[ScalaType2, ScalaType3](
    that: CqlDecoder[ScalaType2]
  )(f: (ScalaType, ScalaType2) => ScalaType3): CqlDecoder[ScalaType3] = new CqlDecoder[ScalaType3] {
    override def decode[S <: GettableByName](structure: S): Either[List[String], ScalaType3] =
      for {
        a <- self.decode(structure)
        b <- that.decode(structure)
      } yield f(a, b)
  }

  def zip[ScalaType2, ScalaType3](that: CqlDecoder[ScalaType2]): CqlDecoder[(ScalaType, ScalaType2)] =
    zipWith(that)((_, _))

}

object CqlDecoder extends MagnoliaDerivationFoCqlDecoder {
  implicit def fromSchema[A](implicit schemaForA: Schema[A]): CqlDecoder[A] = schemaForA match {
    case record: Schema.Record[_] =>
      new CqlDecoder[A] {
        // cached
        val fieldDecoders: Chunk[(String, CqlColumnDecoder[Any])] = record.structure.map { field =>
          val columnDecoder: CqlColumnDecoder[Any] = CqlColumnDecoder.fromSchema(field.schema)
          (field.label, columnDecoder)
        }

        override def decode[S <: GettableByName](structure: S): Either[List[String], A] =
          record.rawConstruct {
            fieldDecoders.map { case (fieldName, decoder) =>
              decoder.decodeFieldByName(structure, fieldName)
            }
          }.left
            .map(List(_))
            .map(_.asInstanceOf[A])
      }
    // extract all the data off the first column
    case enum: Schema.Enum[_]                          => ???
    case collection: Schema.Collection[_, _]           => ???
    case Schema.Transform(codec, f, g, annotations)    => ???
    case Schema.Primitive(standardType, annotations)   => ???
    case Schema.Optional(codec, annotations)           => ???
    case Schema.Fail(message, annotations)             => ???
    case Schema.Tuple(left, right, annotations)        => ???
    case Schema.EitherSchema(left, right, annotations) => ???
    case Schema.Lazy(schema0)                          => ???
    case Schema.Meta(ast, annotations)                 => ???
  }

  final private case class ColumnDecoder[A](columnDecoder: CqlColumnDecoder[A]) extends CqlDecoder[A] {
    override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], A] =
      readByName(structure, name)(columnDecoder)

    override def decode[S <: GettableByName](structure: S): Either[List[String], A] =
      readByIndex(structure, 0)(columnDecoder)
  }

  def apply[ScalaType](implicit decoder: CqlDecoder[ScalaType]): CqlDecoder[ScalaType] = decoder

  implicit def cqlDecoderForCqlColumnDecoder[A](implicit
    columnDecoder: CqlColumnDecoder[A]
  ): CqlDecoder.ColumnDecoder[A] = ColumnDecoder[A](columnDecoder)

  private def readByIndex[A, S <: GettableByName](structure: S, index: Int)(implicit
    columnDecoder: CqlColumnDecoder[A]
  ): Either[List[String], A] =
    try { Right(columnDecoder.decodeFieldByIndex(structure, index)) }
    catch { case e: Exception => Left(List(e.getMessage)) }

  private def readByName[A, S <: GettableByName](structure: S, fieldName: String)(implicit
    columnDecoder: CqlColumnDecoder[A]
  ): Either[List[String], A] =
    try { Right(columnDecoder.decodeFieldByName(structure, fieldName)) }
    catch { case e: Exception => Left(List(e.getMessage)) }

//  implicit def cqlDecoderForTuple2[A, B](implicit
//    columnDecoderForA: CqlColumnDecoder[A],
//    columnDecoderForB: CqlColumnDecoder[B]
//  ): CqlDecoder[(A, B)] = new Typeclass[(A, B)] {
//    override def decode[S <: GettableByName](structure: S): Either[List[String], (A, B)] =
//      combine(readByIndex(structure, 0)(columnDecoderForA), readByIndex(structure, 1)(columnDecoderForB))
//  }
//
//  implicit def cqlDecoderForTuple3[A, B, C](implicit
//    columnDecoderForA: CqlColumnDecoder[A],
//    columnDecoderForB: CqlColumnDecoder[B],
//    columnDecoderForC: CqlColumnDecoder[C]
//  ): CqlDecoder[(A, B, C)] = new Typeclass[(A, B, C)] {
//    override def decode[S <: GettableByName](structure: S): Either[List[String], (A, B, C)] =
//      combine(
//        a = readByIndex(structure, 0)(columnDecoderForA),
//        b = combine(
//          a = readByIndex(structure, 1)(columnDecoderForB),
//          b = readByIndex(structure, 2)(columnDecoderForC)
//        )
//      ).map { case (a, (b, c)) => (a, b, c) }
//  }

  implicit def optionCqlDecoder[A](implicit decoder: CqlDecoder[A]): CqlDecoder[Option[A]] =
    decoder match {
      case ColumnDecoder(columnDecoder) =>
        cqlDecoderForCqlColumnDecoder(CqlColumnDecoder.optionColumnDecoder(columnDecoder))

      case user =>
        new CqlDecoder[Option[A]] {
          override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Option[A]] =
            Option(structure.getUdtValue(name)).map(decoder.decode) match {
              case Some(value) => value.map(Option(_))
              case None        => Right(None)
            }

          override def decode[S <: GettableByName](structure: S): Either[List[String], Option[A]] =
            Option(structure.getUdtValue(0)).map(decoder.decode) match {
              case Some(value) => value.map(Option(_))
              case None        => Right(None)
            }
        }
    }

//  implicit def optionForUdtDecoder[A](implicit decoder: CqlDecoder[A]): CqlDecoder[Option[A]] =
//    new CqlDecoder[Option[A]] {
//      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Option[A]] =
//        Option(structure.getUdtValue(name)).map(decoder.decode) match {
//          case Some(value) => value.map(Option(_))
//          case None        => Right(None)
//        }
//
//      override def decode[S <: GettableByName](structure: S): Either[List[String], Option[A]] =
//        Option(structure.getUdtValue(0)).map(decoder.decode) match {
//          case Some(value) => value.map(Option(_))
//          case None        => Right(None)
//        }
//    }

  implicit def listForUdtDecoder[A](implicit decoder: CqlDecoder[A]): CqlDecoder[List[A]] =
    new CqlDecoder[List[A]] {
      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], List[A]] =
        traverse {
          structure
            .getList[UdtValue](name, classOf[UdtValue])
            .asScala
            .map(decoder.decode(_))
        }

      override def decode[S <: GettableByName](structure: S): Either[List[String], List[A]] =
        traverse {
          structure
            .getList[UdtValue](0, classOf[UdtValue])
            .asScala
            .map(decoder.decode(_))
        }
    }

  implicit def setForUdtDecoder[A](implicit decoderA: CqlDecoder[A]): CqlDecoder[Set[A]] =
    new CqlDecoder[Set[A]] {
      private def extractResult(in: mutable.Set[UdtValue]): Either[List[String], Set[A]] = {
        val errors = mutable.ListBuffer.empty[String]
        val out    = mutable.Set.empty[A]
        in.foreach { v =>
          val either = decoderA.decode(v)
          either match {
            case Right(a) => out += a
            case Left(e)  => errors ++= e
          }
        }
        if (errors.nonEmpty) Left(errors.toList)
        else Right(out.toSet)
      }

      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Set[A]] =
        extractResult(structure.getSet[UdtValue](name, classOf[UdtValue]).asScala)

      override def decode[S <: GettableByName](structure: S): Either[List[String], Set[A]] =
        extractResult(structure.getSet[UdtValue](0, classOf[UdtValue]).asScala)
    }

  implicit def mapForUdtDecoderBothAreUdtValues[A, B](implicit
    decoderA: CqlDecoder[A],
    decoderB: CqlDecoder[B]
  ): CqlDecoder[Map[A, B]] =
    new CqlDecoder[Map[A, B]] {
      private def extractResult(in: mutable.Map[UdtValue, UdtValue]): Either[List[String], Map[A, B]] = {
        val keyValuePairs = mutable.ListBuffer.empty[(A, B)]
        val errors        = mutable.ListBuffer.empty[String]
        in.foreach { case (k, v) =>
          val eitherK  = decoderA.decode(k)
          val eitherV  = decoderB.decode(v)
          val eitherKV = combine(eitherK, eitherV)
          eitherKV match {
            case Right((k, v)) => keyValuePairs += ((k, v))
            case Left(e)       => errors ++= e
          }
        }
        if (errors.nonEmpty) Left(errors.toList)
        else Right(keyValuePairs.toMap)
      }

      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Map[A, B]] =
        extractResult(structure.getMap[UdtValue, UdtValue](name, classOf[UdtValue], classOf[UdtValue]).asScala)

      override def decode[S <: GettableByName](structure: S): Either[List[String], Map[A, B]] =
        extractResult(structure.getMap[UdtValue, UdtValue](0, classOf[UdtValue], classOf[UdtValue]).asScala)
    }

  private def traverse[A](in: mutable.Buffer[Either[List[String], A]]): Either[List[String], List[A]] = {
    val errors: ListBuffer[String] = ListBuffer.empty[String]
    val results: ListBuffer[A]     = ListBuffer.empty[A]
    in.foreach {
      case Right(a) => results += a
      case Left(e)  => errors ++= e
    }
    if (errors.nonEmpty) Left(errors.toList)
    else Right(results.toList)
  }
}

trait MapCqlDecoderForPrimitives {
  implicit def mapForUdtDecoderKeyPrimitiveValueUdtValue[A, B](implicit
    decoderA: CqlColumnDecoder[A],
    decoderB: CqlDecoder[B]
  ): CqlDecoder[Map[A, B]] =
    new CqlDecoder[Map[A, B]] {
      private def extractResult(in: mutable.Map[decoderA.DriverType, UdtValue]): Either[List[String], Map[A, B]] = {
        val keyValuePairs = mutable.ListBuffer.empty[(A, B)]
        val errors        = mutable.ListBuffer.empty[String]
        in.foreach { case (k, v) =>
          val eitherK =
            try { Right(decoderA.convertDriverToScala(k)) }
            catch { case e: Exception => Left(List(e.getMessage)) }
          val eitherV  = decoderB.decode(v)
          val eitherKV = combine(eitherK, eitherV)
          eitherKV match {
            case Right((k, v)) => keyValuePairs += ((k, v))
            case Left(e)       => errors ++= e
          }
        }
        if (errors.nonEmpty) Left(errors.toList)
        else Right(keyValuePairs.toMap)
      }

      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Map[A, B]] =
        extractResult(
          structure.getMap[decoderA.DriverType, UdtValue](name, decoderA.driverClass, classOf[UdtValue]).asScala
        )

      override def decode[S <: GettableByName](structure: S): Either[List[String], Map[A, B]] =
        extractResult(
          structure.getMap[decoderA.DriverType, UdtValue](0, decoderA.driverClass, classOf[UdtValue]).asScala
        )
    }

  implicit def mapForUdtDecoderKeyUdtValueValuePrimitive[A, B](implicit
    decoderA: CqlDecoder[A],
    decoderB: CqlColumnDecoder[B]
  ): CqlDecoder[Map[A, B]] =
    new CqlDecoder[Map[A, B]] {
      private def extractResult(in: mutable.Map[UdtValue, decoderB.DriverType]): Either[List[String], Map[A, B]] = {
        val keyValuePairs = mutable.ListBuffer.empty[(A, B)]
        val errors        = mutable.ListBuffer.empty[String]
        in.foreach { case (k, v) =>
          val eitherK = decoderA.decode(k)
          val eitherV =
            try { Right(decoderB.convertDriverToScala(v)) }
            catch { case e: Exception => Left(List(e.getMessage)) }
          val eitherKV = combine(eitherK, eitherV)
          eitherKV match {
            case Right((k, v)) => keyValuePairs += ((k, v))
            case Left(e)       => errors ++= e
          }
        }
        if (errors.nonEmpty) Left(errors.toList)
        else Right(keyValuePairs.toMap)
      }

      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], Map[A, B]] =
        extractResult(
          structure.getMap[UdtValue, decoderB.DriverType](name, classOf[UdtValue], decoderB.driverClass).asScala
        )

      override def decode[S <: GettableByName](structure: S): Either[List[String], Map[A, B]] =
        extractResult(
          structure.getMap[UdtValue, decoderB.DriverType](0, classOf[UdtValue], decoderB.driverClass).asScala
        )
    }
}

trait MagnoliaDerivationFoCqlDecoder {
  type Typeclass[T] = CqlDecoder[T]

  def join[T](ctx: CaseClass[CqlDecoder, T]): CqlDecoder[T] =
    new CqlDecoder[T] {
      // This would be recursively called
      override def decodeByName[S <: GettableByName](structure: S, name: String): Either[List[String], T] = {
        val udtStructure =
          try { Right(structure.getUdtValue(name)) }
          catch { case e: Exception => Left(List(e.getMessage)) }

        udtStructure.flatMap { structure =>
          ctx.constructEither { field =>
            field.typeclass.decodeByName(structure, field.label).left.map(_.mkString(","))
          }
        }
      }

      override def decode[S <: GettableByName](structure: S): Either[List[String], T] =
        ctx.constructEither { field =>
          field.typeclass.decodeByName(structure, field.label).left.map(_.mkString(", "))
        }
    }

  implicit def derive[A]: CqlDecoder[A] = macro Magnolia.gen[A]
}

/**
 * case class Person(id: Int, address: Address) case class Address(city: String,
 * ...)
 *
 * // Top level -> only want CqlDecoder -> take a CqlColumnDecoder promote
 * CqlDecoder // Recursive case -> only want a CqlColumnDecoder
 */
