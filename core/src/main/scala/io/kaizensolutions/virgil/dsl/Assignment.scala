package io.kaizensolutions.virgil.dsl

import io.kaizensolutions.virgil.codecs.CqlRowComponentEncoder
import io.kaizensolutions.virgil.internal.BindMarkerName

sealed trait Assignment
object Assignment {
  final case class AssignValue[A](columnName: BindMarkerName, value: A, ev: CqlRowComponentEncoder[A])
      extends Assignment

  final case class UpdateCounter(columnName: BindMarkerName, offset: Long) extends Assignment

  final case class PrependListItems[A](
    columnName: BindMarkerName,
    values: IndexedSeq[A],
    ev: CqlRowComponentEncoder[List[A]]
  ) extends Assignment

  final case class RemoveListItems[A](
    columnName: BindMarkerName,
    values: IndexedSeq[A],
    ev: CqlRowComponentEncoder[List[A]]
  ) extends Assignment

  final case class AppendListItems[A](
    columnName: BindMarkerName,
    values: IndexedSeq[A],
    ev: CqlRowComponentEncoder[List[A]]
  ) extends Assignment

  final case class AssignValueAtListIndex[A](
    columnName: BindMarkerName,
    index: Int,
    value: A,
    ev: CqlRowComponentEncoder[A]
  ) extends Assignment

  final case class AddSetItems[A](
    columnName: BindMarkerName,
    value: IndexedSeq[A],
    ev: CqlRowComponentEncoder[Set[A]]
  ) extends Assignment

  final case class RemoveSetItems[A](
    columnName: BindMarkerName,
    value: IndexedSeq[A],
    ev: CqlRowComponentEncoder[Set[A]]
  ) extends Assignment

  final case class AppendMapItems[K, V](
    columnName: BindMarkerName,
    entries: IndexedSeq[(K, V)],
    ev: CqlRowComponentEncoder[Map[K, V]]
  ) extends Assignment

  final case class RemoveMapItemsByKey[K](
    columnName: BindMarkerName,
    keys: IndexedSeq[K],
    evK: CqlRowComponentEncoder[List[K]]
  ) extends Assignment

  final case class AssignValueAtMapKey[K, V](
    columnName: BindMarkerName,
    key: K,
    value: V,
    evK: CqlRowComponentEncoder[K],
    evV: CqlRowComponentEncoder[V]
  ) extends Assignment
}
trait AssignmentSyntax {
  implicit class AssignmentStringOps(rawColumn: String) {
    def :=[A](value: A)(implicit ev: CqlRowComponentEncoder[A]): Assignment.AssignValue[A] =
      Assignment.AssignValue(BindMarkerName.make(rawColumn), value, ev)

    def +=(value: Long): Assignment.UpdateCounter =
      Assignment.UpdateCounter(BindMarkerName.make(rawColumn), value)

    def -=(value: Long): Assignment.UpdateCounter =
      Assignment.UpdateCounter(BindMarkerName.make(rawColumn), -value)

    def prependItemsToList[A](value: A, values: A*)(implicit
      ev: CqlRowComponentEncoder[List[A]]
    ): Assignment.PrependListItems[A] = {
      val _ = ev
      Assignment.PrependListItems(
        BindMarkerName.make(rawColumn),
        IndexedSeq.concat(value +: values),
        ev
      )
    }

    def appendItemsToList[A](value: A, values: A*)(implicit
      ev: CqlRowComponentEncoder[List[A]]
    ): Assignment.AppendListItems[A] = {
      val _ = ev
      Assignment.AppendListItems(
        BindMarkerName.make(rawColumn),
        IndexedSeq.concat(value +: values),
        ev
      )
    }

    def removeListItems[A](value: A, values: A*)(implicit
      ev: CqlRowComponentEncoder[List[A]]
    ): Assignment.RemoveListItems[A] = {
      val _ = ev
      Assignment.RemoveListItems(
        BindMarkerName.make(rawColumn),
        IndexedSeq.concat(value +: values),
        ev
      )
    }

    def assignAt[A](
      index: Int
    )(value: A)(implicit ev: CqlRowComponentEncoder[A]): Assignment.AssignValueAtListIndex[A] = {
      val _ = ev
      Assignment.AssignValueAtListIndex(BindMarkerName.make(rawColumn), index, value, ev)
    }

    def addSetItem[A](value: A, values: A*)(implicit ev: CqlRowComponentEncoder[Set[A]]): Assignment.AddSetItems[A] = {
      val _ = ev
      Assignment.AddSetItems(BindMarkerName.make(rawColumn), IndexedSeq.concat(value +: values), ev)
    }

    def removeSetItems[A](value: A, values: A*)(implicit
      ev: CqlRowComponentEncoder[Set[A]]
    ): Assignment.RemoveSetItems[A] = {
      val _ = ev
      Assignment.RemoveSetItems(BindMarkerName.make(rawColumn), IndexedSeq.concat(value +: values), ev)
    }

    def appendItemsToMap[K, V](
      entry: (K, V),
      entries: (K, V)*
    )(implicit ev: CqlRowComponentEncoder[Map[K, V]]): Assignment.AppendMapItems[K, V] =
      Assignment.AppendMapItems(
        BindMarkerName.make(rawColumn),
        IndexedSeq.concat(entry +: entries),
        ev
      )

    def removeItemsFromMapByKey[K](key: K, keys: K*)(implicit
      evK: CqlRowComponentEncoder[List[K]]
    ): Assignment.RemoveMapItemsByKey[K] = {
      val _ = evK
      Assignment.RemoveMapItemsByKey(BindMarkerName.make(rawColumn), IndexedSeq.concat(key +: keys), evK)
    }

    def assignAt[K, V](key: K)(
      value: V
    )(implicit evK: CqlRowComponentEncoder[K], evV: CqlRowComponentEncoder[V]): Assignment.AssignValueAtMapKey[K, V] = {
      val _ = (evK, evV)
      Assignment.AssignValueAtMapKey(BindMarkerName.make(rawColumn), key, value, evK, evV)
    }
  }
}
