package io.kaizensolutions.virgil

import io.kaizensolutions.virgil.configuration.PageState
import zio.Chunk

final case class Paged[A](data: Chunk[A], pageState: Option[PageState])
