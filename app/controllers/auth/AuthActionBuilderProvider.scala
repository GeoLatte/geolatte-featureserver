package controllers.auth

import play.api.mvc.PlayBodyParsers

import scala.concurrent.ExecutionContext

trait AuthActionBuilderProvider {
  def apply(rights: Rights, parsers: PlayBodyParsers)(implicit ec: ExecutionContext): AuthActionBuilder
}

object AuthActionBuilderProvider {

  @volatile private var _provider: AuthActionBuilderProvider = DefaultAuthActionBuilderProvider

  def provider: AuthActionBuilderProvider = _provider

  def setProvider(p: AuthActionBuilderProvider): Unit = {
    _provider = p
  }

  def apply(rights: Rights, parsers: PlayBodyParsers)(implicit ec: ExecutionContext): AuthActionBuilder =
    _provider(rights, parsers)
}
