package com.caesars.virgil

sealed trait CassandraBatchType
object CassandraBatchType {
  case object Logged   extends CassandraBatchType
  case object Unlogged extends CassandraBatchType
  case object Counter  extends CassandraBatchType
}
