package controllers

import play.api.mvc.{Controller, Result}
import controllers.Exceptions.{CollectionNotFoundException, DatabaseNotFoundException}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 10/11/13
 */
trait AbstractNoSqlController extends Controller {


  def commonExceptionHandler(db : String, col : String = "") : PartialFunction[Throwable, Result] = {
    case ex: DatabaseNotFoundException => NotFound(s"Database $db does not exist.")
    case ex: CollectionNotFoundException => NotFound(s"Collection $db/$col does not exist.")
    case ex: Throwable => InternalServerError(s"${ex.getMessage}")
  }

}
