package controllers

import play.api.mvc.{Action, Controller}

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {

    def loadInto(db: String, col: String) = Action {
      Ok("placeholder")
    }

}
