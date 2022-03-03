package io.kaizensolutions.virgil.annotations

import scala.annotation.StaticAnnotation

final case class CqlSubtype(name: String) extends StaticAnnotation
object CqlSubtype {
  @inline def extract(annotations: Seq[Any]): Option[String] =
    annotations.collectFirst { case CqlSubtype(name) => name }
}
