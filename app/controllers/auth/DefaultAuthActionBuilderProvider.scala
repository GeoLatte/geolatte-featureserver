package controllers.auth

import play.api.mvc.PlayBodyParsers

import scala.concurrent.ExecutionContext

object DefaultAuthActionBuilderProvider extends AuthActionBuilderProvider {

  override def apply(rights: Rights, parsers: PlayBodyParsers)(implicit ec: ExecutionContext): AuthActionBuilder =
    new DefaultAuthActionBuilder(rights, parsers)
}
