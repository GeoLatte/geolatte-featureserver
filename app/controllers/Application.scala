package controllers

import play.api.mvc._

object Application extends Controller {

  def index = Action {
    Results.Redirect("/index.html")
  }
  
}