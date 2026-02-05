package controllers.auth

import play.api.mvc.PlayBodyParsers

import scala.concurrent.ExecutionContext

trait AuthController {
  def parsers: PlayBodyParsers

  def ifHasRights(rights: Rights)(implicit ec: ExecutionContext): AuthActionBuilder =
    AuthActionBuilderProvider(rights, parsers)
}
