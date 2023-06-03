package io.kaizensolutions.virgil

import fs2.Chunk
import io.kaizensolutions.virgil.configuration.PageState

final case class Paged[A](data: Chunk[A], pageState: Option[PageState])
