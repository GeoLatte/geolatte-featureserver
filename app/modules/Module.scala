package modules

import com.google.inject.AbstractModule
import featureserver.Repository
import featureserver.postgresql.PostgresqlRepository
import metrics.{FeatureServerMetrics, Metrics}
import play.api.{Configuration, Environment}

/**
  * Created by Karel Maesen, Geovise BVBA on 05/09/16.
  */
class RepoModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = bind(classOf[Repository]).to(classOf[PostgresqlRepository]).asEagerSingleton

}

class MetricsModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = bind(classOf[Metrics]).to(classOf[FeatureServerMetrics]).asEagerSingleton
}