package com.caesars.virgil.cql

import com.caesars.virgil.{CassandraInteraction, ValueInCql}
import com.caesars.virgil.codecs.{Reader, Writer}
import com.datastax.oss.driver.api.core.cql.Row

import scala.collection.immutable.ListMap

/**
 * CqlStringInterpolator is an interpolator for CQL strings
 */
class CqlStringInterpolator(ctx: StringContext) {
  private def replaceValueWithQuestionMark(
    strings: Iterator[String],
    expressions: Iterator[ValueInCql]
  ): (String, ListMap[String, ValueInCql]) = {
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
    (query, data)
  }

  def apply(values: ValueInCql*): CassandraInteraction.Query[Row] = {
    val (query, data) = replaceValueWithQuestionMark(ctx.parts.iterator, values.iterator)
    CassandraInteraction.Query(query, data, Reader[Row])
  }
}
