package controllers.auth

import play.api.mvc.{ Request, WrappedRequest }

abstract class AuthRequestWithRights[A](request: Request[A])
  extends WrappedRequest[A](request) {
  def rights: Rights
}
