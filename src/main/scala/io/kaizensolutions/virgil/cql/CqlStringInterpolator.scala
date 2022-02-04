package io.kaizensolutions.virgil.cql

import scala.collection.immutable.ListMap

/**
 * CqlStringInterpolator is an interpolator for CQL strings
 *
 * For example:
 * {{{
 * val value1: Int    = 1
 * val value2: String = "test"
 * val query          = cql"SELECT * FROM example_table WHERE col1 = $value1 AND col2 = $value2"
 * }}}
 *
 * Produces the following datatype:
 * {{{
 * CqlInterpolatedString(
 *   query = "SELECT * FROM example_table WHERE col1 = :param0 AND col2 = :param1",
 *   dataToBeBound = ListMap(
 *     "param0" -> ValueInCql {
 *       type ScalaType = Int
 *       value = 1
 *       writer = Writer.intWriter
 *     },
 *     "param1" -> ValueInCql {
 *       type ScalaType = String
 *       value = "test"
 *       writer = Writer.stringWriter
 *     }
 *   )
 * )
 * }}}
 */
class CqlStringInterpolator(ctx: StringContext) {
  private def replaceValueWithQuestionMark(
    strings: Iterator[String],
    expressions: Iterator[ValueInCql]
  ): CqlInterpolatedString = {
    val sb                  = new StringBuilder
    var expressionCounter   = 0
    val parameterNamePrefix = "param"
    val colon               = ":"
    var data                = ListMap.empty[String, ValueInCql]
    while (strings.hasNext) {
      sb.append(strings.next())
      if (expressions.hasNext) {
        val param = s"$parameterNamePrefix$expressionCounter"
        val entry = expressions.next()
        sb.append(colon + param)
        data = data + (param -> entry)
        expressionCounter += 1
      }
    }
    while (expressions.hasNext) {
      val param = s"$parameterNamePrefix$expressionCounter"
      val entry = expressions.next()
      sb.append(colon + param)
      expressionCounter += 1
      data = data + (param -> entry)
    }
    val query = sb.toString
    CqlInterpolatedString(queryString = query, dataToBeBound = data)
  }

  def apply(values: ValueInCql*): CqlInterpolatedString =
    replaceValueWithQuestionMark(ctx.parts.iterator, values.iterator)
}
