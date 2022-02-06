package io.kaizensolutions.virgil.dsl

sealed trait BinaryOperator { self =>
  def render: String = self match {
    case BinaryOperator.Equal              => "="
    case BinaryOperator.NotEqual           => "!="
    case BinaryOperator.GreaterThan        => ">"
    case BinaryOperator.GreaterThanOrEqual => ">="
    case BinaryOperator.LessThan           => "<"
    case BinaryOperator.LessThanOrEqual    => "<="
    case BinaryOperator.Like               => "LIKE"
    case BinaryOperator.In                 => "IN"
  }
}
object BinaryOperator {
  case object Equal              extends BinaryOperator
  case object NotEqual           extends BinaryOperator
  case object GreaterThan        extends BinaryOperator
  case object GreaterThanOrEqual extends BinaryOperator
  case object LessThan           extends BinaryOperator
  case object LessThanOrEqual    extends BinaryOperator
  case object Like               extends BinaryOperator
  case object In                 extends BinaryOperator
}
