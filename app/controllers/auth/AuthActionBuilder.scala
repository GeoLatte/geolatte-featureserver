package controllers.auth

import play.api.mvc._

import scala.concurrent.{ ExecutionContext, Future }

abstract class AuthActionBuilder(val requiredRights: Rights)(implicit val executionContext: ExecutionContext)
  extends ActionBuilder[AuthRequestWithRights, AnyContent]
  with ActionTransformer[Request, AuthRequestWithRights] {

  def transform[A](request: Request[A]): Future[AuthRequestWithRights[A]]
}
