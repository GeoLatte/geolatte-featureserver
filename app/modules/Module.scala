package modules

import com.google.inject.AbstractModule
import nosql.Repository
import nosql.postgresql.PostgresqlRepository
import play.api.{Configuration, Environment}

/**
  * Created by Karel Maesen, Geovise BVBA on 05/09/16.
  */
class RepoModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = bind(classOf[Repository]).to(classOf[PostgresqlRepository]).asEagerSingleton

}
