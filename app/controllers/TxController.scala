package controllers

import play.api.mvc.{Action, Controller}
import org.geolatte.nosql.json.ReactiveGeoJson
import repositories.MongoBufferedWriter

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 7/25/13
 */
object TxController extends Controller {

  def loadInto(db: String, col: String) = {
    val writer = try {
      val writer = new MongoBufferedWriter(db, col)
      Some(ReactiveGeoJson.bodyParser(writer))
    } catch {
      case ex: Throwable => None
    }
    val parser = parse.using { rh =>
      writer.getOrElse(parse.error(NotFound(s"$db/$col does not exist or is not a spatial collection")))
    }
    Action(parser) {
        request => Ok(request.body.msg)
    }
  }

}
