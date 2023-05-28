package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.configuration.PageState

final case class Paged[A](data: IndexedSeq[A], pageState: Option[PageState])
