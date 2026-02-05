package controllers.auth

import play.api.mvc._

import scala.concurrent.{ ExecutionContext, Future }

class DefaultAuthActionBuilder(requiredRights: Rights, parsers: PlayBodyParsers)(implicit ec: ExecutionContext)
  extends AuthActionBuilder(requiredRights) {

  override def parser: BodyParser[AnyContent] = parsers.anyContent

  override def transform[A](request: Request[A]): Future[AuthRequestWithRights[A]] = {
    Future.successful(new AuthRequestWithRights[A](request) {
      override def rights: Rights = requiredRights
    })
  }
}
