package controllers

import play.api.mvc._

class Application extends Controller {

  def index = Action {
    Results.Redirect("/index.html")
  }
  
}