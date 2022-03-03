package io.kaizensolutions.virgil.annotations

import scala.annotation.StaticAnnotation

final case class CqlDiscriminator(name: String) extends StaticAnnotation
object CqlDiscriminator {
  @inline def extract(annotations: Seq[Any]): Option[String] =
    annotations.collectFirst { case CqlDiscriminator(name) => name }
}
