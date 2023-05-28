package io.kaizensolutions.virgil.cql

import scala.collection.mutable

/**
 * Gathers the interpolated string for further composition so it can be built
 * into CQL.
 *
 * @param ctx
 */
final class CqlStringInterpolator(ctx: StringContext) {
  private def associate(
    strings: Iterator[String],
    expressions: Iterator[ValueInCql]
  ): CqlInterpolatedString = {
    val acc: mutable.ArrayBuilder[CqlPartRepr] = mutable.ArrayBuilder.make[CqlPartRepr]
    while (strings.hasNext) {
      if (expressions.hasNext) {
        val q = strings.next()
        val m = expressions.next()
        acc += CqlPartRepr.Pair(q, m)
      } else {
        val q = strings.next()
        acc += CqlPartRepr.Query(q)
      }
    }
    while (expressions.hasNext) {
      val m = expressions.next()
      acc += CqlPartRepr.Marker(m)
    }
    val representation = acc.result()
    CqlInterpolatedString(representation)
  }

  def apply(values: ValueInCql*): CqlInterpolatedString =
    associate(ctx.parts.iterator, values.iterator)
}
