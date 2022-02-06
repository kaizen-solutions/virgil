package io.kaizensolutions.virgil

package object dsl extends RelationSyntax {
  val insert: Insert.type                         = Insert
  val update: String => Update[UpdateState.Empty] = Update.apply
}
