package modules

import com.google.inject.AbstractModule
import nosql.Repository
import nosql.mongodb.MongoDBRepository
import nosql.postgresql.PostgresqlRepository
import play.api.{Configuration, Environment}
import javax.inject._

/**
  * Created by Karel Maesen, Geovise BVBA on 05/09/16.
  */
class RepoModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = {
    configuration.getString("fs.db").getOrElse("postgresql") match {
      case "mongodb"    => bind(classOf[Repository]).to(classOf[MongoDBRepository]).asEagerSingleton
      case "postgresql" => bind(classOf[Repository]).to(classOf[PostgresqlRepository]).asEagerSingleton
      case _            => sys.error("Configured with Unsupported database")
    }
  }

}
