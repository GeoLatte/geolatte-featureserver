package nosql.postgresql

import nosql.{FeatureTransformers, Metadata, FeatureWriter}
import org.geolatte.geom.{Polygon, Envelope}
import play.api.Logger
import play.api.libs.json.{JsSuccess, Reads, JsObject}

import scala.concurrent.Future

/**
 * A Writer for
 * Created by Karel Maesen, Geovise BVBA on 15/12/14.
 */
case class PGWriter(db: String, collection: String) extends FeatureWriter {

  import config.AppExecutionContexts.streamContext

  lazy val metadata : Future[Metadata] = PostgresqlRepository.metadata(db, collection)

  lazy val envelopeReads: Future[Reads[Polygon]] = metadata.map{ md =>
    FeatureTransformers.envelopeTransformer(md.envelope)
  }

  override def add(features: Seq[JsObject]): Future[Long] =
    if(features.isEmpty) Future.successful(0)
    else {
      envelopeReads.flatMap { evr =>
        val docs : Seq[(JsObject, Polygon)] = features.map {f =>
          (f, f.asOpt[Polygon](evr))
        } collect {
          case (f, Some(env)) => (f, env)
        }
        val fLng = PostgresqlRepository.insert(db, collection, docs)
//        Logger.debug(" Flushing data to postgresql (" + docs.size + " features)")
//        val fLng = Future.sequence( for( (f,e) <- docs) yield PostgresqlRepository.insert(db, collection, f,e) )
//          .map(s => s.foldLeft(0L)( (a,b) => if (b) a+1 else a ) )

        fLng.onSuccess {
          case num => Logger.info(s"Successfully inserted $num features")
        }
        fLng.recover {
          case t : Throwable => { Logger.warn(s"Insert failed with error: ${t.getMessage}"); 0l }
        }
        fLng
      }
    }

}
