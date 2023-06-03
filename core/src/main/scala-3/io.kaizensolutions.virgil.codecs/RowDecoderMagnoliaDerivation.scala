package io.kaizensolutions.virgil.codecs

import scala.deriving.Mirror
import com.datastax.oss.driver.api.core.cql.Row
import io.kaizensolutions.virgil.annotations.CqlColumn
import magnolia1.*

// Note: Fully automatic derivation is not yet present in Scala 3 just yet (because I haven't figured out how to do it yet)
trait RowDecoderMagnoliaDerivation extends ProductDerivation[CqlRowDecoder]:
  final def join[T](ctx: CaseClass[Typeclass, T]): CqlRowDecoder.Object[T] = (row: Row) =>
    ctx.construct { p =>
      val fieldName = CqlColumn.extractFieldName(p.annotations).getOrElse(p.label)
      p.typeclass.decodeByFieldName(row, fieldName)
    }

  inline def derive[A](using Mirror.Of[A]): CqlRowDecoder.Object[A] =
    derived.asInstanceOf[CqlRowDecoder.Object[A]]
