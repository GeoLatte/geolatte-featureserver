package controllers

import config.AppExecutionContexts

object IndexController extends AbstractNoSqlController{

  import AppExecutionContexts._

  def put(db: String, collection: String, index: String) = play.mvc.Results.TODO

  def list(db: String, collection: String) = play.mvc.Results.TODO

  def get(db: String, collection: String, index: String) = play.mvc.Results.TODO

  def delete(db: String, collection: String, index: String) = play.mvc.Results.TODO
}