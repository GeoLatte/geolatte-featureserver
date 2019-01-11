package persistence.postgresql

import Exceptions.InvalidParamsException
import org.geolatte.geom.Geometry
import persistence.{ FeatureWriter, GeoJsonFormats, Metadata }
import play.api.Logger
import play.api.libs.json.{ JsObject, Reads }

import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

/**
 * A Writer for
 * Created by Karel Maesen, Geovise BVBA on 15/12/14.
 */
case class PGWriter(repo: PostgresqlRepository, db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  lazy val metadata: Future[Metadata] = repo.metadata(db, collection)

  lazy val reads: Future[(Reads[Geometry], Reads[JsObject])] = metadata.map { md =>
    (
      GeoJsonFormats.geoJsonGeometryReads,
      GeoJsonFormats.featureValidator(md.idType)
    )
  }

  override def insert(features: Seq[JsObject]): Future[Int] =
    write(features, repo.batchInsert)

  override def upsert(features: Seq[JsObject]): Future[Int] =
    write(features, repo.upsert)

  private def write(features: Seq[JsObject], writer: (String, String, Seq[(JsObject, Geometry)]) => Future[Int]): Future[Int] =
    if (features.isEmpty) Future.successful(0)
    else {
      reads.flatMap {
        case (geometryReader, validator) =>
          val docsTry: Try[Seq[(JsObject, Geometry)]] =
            features.foldLeft(Success(Seq()): Try[Seq[(JsObject, Geometry)]]) {
              case (f @ Failure(_), _) => f // continue with failure
              case (Success(acc), feature) =>
                for {
                  validatedFeature <- Try(feature.as(validator)).recover {
                    case _: play.api.libs.json.JsResultException =>
                      throw InvalidParamsException("Invalid Json object")
                  }
                  featureGeometry <- Try(feature.as[Geometry](geometryReader)).recover {
                    case _: play.api.libs.json.JsResultException =>
                      throw InvalidParamsException("Invalid Geometry in Json object")
                  }
                } yield acc :+ (validatedFeature, featureGeometry)
            }
          val fInt =
            for {
              docs <- Future.fromTry(docsTry)
              i <- writer(db, collection, docs)
            } yield i

          fInt.onComplete {
            case Success(num) => Logger.debug(s"Successfully written $num features")
            case Failure(t) => Logger.warn(s"Write failed with error: ${t.getMessage}")
          }

          fInt
      }
    }

}
