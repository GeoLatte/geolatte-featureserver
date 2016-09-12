package config

import play.api.libs.concurrent.Akka
import scala.concurrent.ExecutionContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/28/13
 */
object AppExecutionContexts {

  //TODO -- figure out how to configure the "Streaming" Context, if still appropriate
  implicit val streamContext: ExecutionContext = scala.concurrent.ExecutionContext.global

}
