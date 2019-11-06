package controllers

import com.google.inject.Inject
import play.api.mvc._

class Application @Inject() () extends InjectedController {

  def index = Action {
    Results.Redirect("/index.html")
  }

}