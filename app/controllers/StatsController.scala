package controllers

import javax.inject.Inject

import metrics.Instrumentation
import persistence.RepoHealth
import play.api.mvc.{ Action, Controller }
import play.api.libs.json._
import Formats._

/**
 * Created by Karel Maesen, Geovise BVBA on 15/03/2018.
 */
class StatsController @Inject() (val repoHealth: RepoHealth, val instrumentation: Instrumentation) extends Controller {

  import config.AppExecutionContexts._

  def getTableStats = Action.async {
    repoHealth.getTableStats.map(res => Ok(Json.toJson(res)))
      .recover { case ex: Throwable => InternalServerError(ex.getMessage) }
  }

  def getActivityStats = Action.async {
    repoHealth.getActivityStats
      .map(res => Ok(Json.toJson(res)))
      .recover { case ex: Throwable => InternalServerError(ex.getMessage) }
  }
}
