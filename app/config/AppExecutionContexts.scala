package config

import play.api.libs.concurrent.Akka
import scala.concurrent.ExecutionContext

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/28/13
 */
object AppExecutionContexts {

  import play.api.Play.current

  implicit val streamContext : ExecutionContext = Akka.system.dispatchers.lookup("play.akka.actor.stream-dispatcher")

}
