package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.Writer
import io.kaizensolutions.virgil.internal.BindMarkerName
import zio.NonEmptyChunk

sealed trait Assignment
object Assignment {
  final case class AssignValue[A](columnName: BindMarkerName, value: A, ev: Writer[A]) extends Assignment

  final case class UpdateCounter(columnName: BindMarkerName, offset: Long) extends Assignment

  final case class PrependListItems[A](columnName: BindMarkerName, values: NonEmptyChunk[A], ev: Writer[List[A]])
      extends Assignment

  final case class RemoveListItems[A](columnName: BindMarkerName, values: NonEmptyChunk[A], ev: Writer[List[A]])
      extends Assignment

  final case class AppendListItems[A](columnName: BindMarkerName, values: NonEmptyChunk[A], ev: Writer[List[A]])
      extends Assignment

  final case class AssignValueAtListIndex[A](columnName: BindMarkerName, index: Int, value: A, ev: Writer[A])
      extends Assignment

  final case class AddSetItems[A](columnName: BindMarkerName, value: NonEmptyChunk[A], ev: Writer[Set[A]])
      extends Assignment

  final case class RemoveSetItems[A](columnName: BindMarkerName, value: NonEmptyChunk[A], ev: Writer[Set[A]])
      extends Assignment

  final case class AppendMapItems[K, V](
    columnName: BindMarkerName,
    entries: NonEmptyChunk[(K, V)],
    ev: Writer[Map[K, V]]
  ) extends Assignment

  final case class RemoveMapItemsByKey[K](
    columnName: BindMarkerName,
    keys: NonEmptyChunk[K],
    evK: Writer[List[K]]
  ) extends Assignment

  final case class AssignValueAtMapKey[K, V](
    columnName: BindMarkerName,
    key: K,
    value: V,
    evK: Writer[K],
    evV: Writer[V]
  ) extends Assignment
}
trait AssignmentSyntax {
  implicit class AssignmentStringOps(rawColumn: String) {
    def :=[A](value: A)(implicit ev: Writer[A]): Assignment.AssignValue[A] =
      Assignment.AssignValue(BindMarkerName.make(rawColumn), value, ev)

    def +[A](value: Long)(implicit ev: Writer[A]): Assignment.UpdateCounter = {
      val _ = ev
      Assignment.UpdateCounter(BindMarkerName.make(rawColumn), value)
    }

    def -[A](value: Long)(implicit ev: Writer[A]): Assignment.UpdateCounter = {
      val _ = ev
      Assignment.UpdateCounter(BindMarkerName.make(rawColumn), -value)
    }

    def prependItemsToList[A](value: A, values: A*)(implicit ev: Writer[List[A]]): Assignment.PrependListItems[A] = {
      val _ = ev
      Assignment.PrependListItems(BindMarkerName.make(rawColumn), NonEmptyChunk(value, values: _*), ev)
    }

    def appendItemsToList[A](value: A, values: A*)(implicit ev: Writer[List[A]]): Assignment.AppendListItems[A] = {
      val _ = ev
      Assignment.AppendListItems(BindMarkerName.make(rawColumn), NonEmptyChunk(value, values: _*), ev)
    }

    def removeListItems[A](value: A, values: A*)(implicit ev: Writer[List[A]]): Assignment.RemoveListItems[A] = {
      val _ = ev
      Assignment.RemoveListItems(BindMarkerName.make(rawColumn), NonEmptyChunk(value, values: _*), ev)
    }

    def assignAt[A](index: Int)(value: A)(implicit ev: Writer[A]): Assignment.AssignValueAtListIndex[A] = {
      val _ = ev
      Assignment.AssignValueAtListIndex(BindMarkerName.make(rawColumn), index, value, ev)
    }

    def addSetItem[A](value: A, values: A*)(implicit ev: Writer[Set[A]]): Assignment.AddSetItems[A] = {
      val _ = ev
      Assignment.AddSetItems(BindMarkerName.make(rawColumn), NonEmptyChunk(value, values: _*), ev)
    }

    def removeSetItems[A](value: A, values: A*)(implicit ev: Writer[Set[A]]): Assignment.RemoveSetItems[A] = {
      val _ = ev
      Assignment.RemoveSetItems(BindMarkerName.make(rawColumn), NonEmptyChunk(value, values: _*), ev)
    }

    def appendItemsToMap[K, V](
      entry: (K, V),
      entries: (K, V)*
    )(implicit ev: Writer[Map[K, V]]): Assignment.AppendMapItems[K, V] =
      Assignment.AppendMapItems(BindMarkerName.make(rawColumn), NonEmptyChunk(entry, entries: _*), ev)

    def removeItemsFromMapByKey[K](key: K, keys: K*)(implicit
      evK: Writer[List[K]]
    ): Assignment.RemoveMapItemsByKey[K] = {
      val _ = evK
      Assignment.RemoveMapItemsByKey(BindMarkerName.make(rawColumn), NonEmptyChunk(key, keys: _*), evK)
    }

    def assignAt[K, V](key: K)(
      value: V
    )(implicit evK: Writer[K], evV: Writer[V]): Assignment.AssignValueAtMapKey[K, V] = {
      val _ = (evK, evV)
      Assignment.AssignValueAtMapKey(BindMarkerName.make(rawColumn), key, value, evK, evV)
    }
  }
}
