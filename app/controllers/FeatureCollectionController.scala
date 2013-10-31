package controllers

import scala.language.reflectiveCalls

import org.geolatte.geom.Envelope
import org.geolatte.geom.crs.CrsId
import play.api.mvc._
import play.api.Logger
import nosql.mongodb.{Metadata, MongoRepository}
import config.AppExecutionContexts
import scala.Some
import utilities.QueryParam
import nosql.Exceptions._



object FeatureCollectionController extends AbstractNoSqlController {

  import AppExecutionContexts.streamContext
  import Formats._

  object QueryParams {
    //we leave bbox as a String parameter because an Envelope needs a CrsId
    val BBOX = QueryParam("bbox", (s: String) => Some(s))
  }


  def query(db: String, collection: String) = repositoryAction ( repo =>
    implicit request => {
        implicit val queryStr = request.queryString
        Logger.info(s"Query string $queryStr on $db, collection $collection")
        repo.metadata(db, collection).flatMap(md =>
          qetQueryResult(db, collection, md).map[Result](x => x)
        ).recover {
          case ex: InvalidQueryException => BadRequest(s"${ex.getMessage}")
        }.recover (commonExceptionHandler(db, collection))
    })

  def download(db: String, collection: String) = repositoryAction { repo =>
    implicit request => {
        Logger.info(s"Downloading $db/$collection.")
        repo.getData(db, collection).map[Result](x => x).recover {
          commonExceptionHandler(db, collection)
        }
      }
  }

  private def qetQueryResult(db: String, collection: String, smd: Metadata)(implicit queryStr: Map[String, Seq[String]]) = {
    Bbox(QueryParams.BBOX.extractOrElse(""), smd.envelope.getCrsId) match {
      case Some(window) =>
        MongoRepository.query(db, collection, window)
      case None => throw new InvalidQueryException(s"BadRequest: No or invalid bbox parameter in query string.")
    }
  }



  object Bbox {

    private val bbox_pattern = "(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+),(-*[\\.\\d]+)".r

    def apply(s: String, crs: CrsId): Option[Envelope] = {
      s match {
        case bbox_pattern(minx, miny, maxx, maxy) => {
          try {
            val env = new Envelope(minx.toDouble, miny.toDouble, maxx.toDouble, maxy.toDouble, crs)
            if (!env.isEmpty) Some(env)
            else None
          } catch {
            case _: Throwable => None
          }
        }
        case _ => None
      }
    }
  }


}