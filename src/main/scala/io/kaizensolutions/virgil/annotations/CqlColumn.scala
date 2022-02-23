package io.kaizensolutions.virgil.annotations

import scala.annotation.StaticAnnotation

final case class CqlColumn(name: String) extends StaticAnnotation

object CqlColumn {
  @inline def extractFieldName(annotations: Seq[Any]): Option[String] =
    annotations.collectFirst { case CqlColumn(name) => name }
}
