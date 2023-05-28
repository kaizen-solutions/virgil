package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.configuration.PageState
import fs2.Chunk

final case class Paged[A](data: Chunk[A], pageState: Option[PageState])
