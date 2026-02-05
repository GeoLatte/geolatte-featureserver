package controllers.auth

sealed trait Rights

object Rights {
  case object Read extends Rights

  case object Write extends Rights

  case object Admin extends Rights
}
