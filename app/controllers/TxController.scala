package controllers

import play.api.mvc.{Action, Controller}
import org.geolatte.nosql.json.ReactiveGeoJson

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {

    def loadInto(db: String, col: String) = Action(ReactiveGeoJson.bodyParser) {
      request => Ok(request.body.msg)
    }

}
