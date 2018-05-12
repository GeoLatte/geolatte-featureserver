package persistence.postgresql

import persistence.{ FeatureTransformers, FeatureWriter, Metadata, Repository }
import org.geolatte.geom.{ Geometry, Polygon }
import play.api.Logger
import play.api.libs.json.{ JsObject, Reads }

import scala.concurrent.Future

/**
 * A Writer for
 * Created by Karel Maesen, Geovise BVBA on 15/12/14.
 */
case class PGWriter(repo: PostgresqlRepository, db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  lazy val metadata: Future[Metadata] = repo.metadata(db, collection)

  lazy val reads: Future[(Reads[Geometry], Reads[JsObject])] = metadata.map { md =>
    (
      FeatureTransformers.envelopeTransformer(md.envelope),
      FeatureTransformers.validator(md.idType)
    )
  }

  override def add(features: Seq[JsObject]): Future[Int] =
    if (features.isEmpty) Future.successful(0)
    else {
      reads.flatMap {
        case (evr, validator) =>
          val docs: Seq[(JsObject, Geometry)] = features.map { f =>
            (f.asOpt(validator), f.asOpt[Geometry](evr))
          } collect {
            case (Some(f), Some(env)) => (f, env)
          }
          val fInt = repo.batchInsert(db, collection, docs)
          fInt.onSuccess {
            case num => Logger.debug(s"Successfully inserted $num features")
          }
          fInt.recover {
            case t: Throwable =>
              Logger.warn(s"Insert failed with error: ${t.getMessage}")
              0l
          }
          fInt
      }
    }

}
