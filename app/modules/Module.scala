package modules

import com.google.inject.AbstractModule
import persistence.{ RepoHealth, Repository }
import persistence.postgresql.PostgresqlRepository
import metrics.{ FeatureServerMetrics, Instrumentation, Metrics, StdInstrumentation }
import play.api.{ Configuration, Environment }

/**
 * Created by Karel Maesen, Geovise BVBA on 05/09/16.
 */
class RepoModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = {
    bind(classOf[Repository]).to(classOf[PostgresqlRepository]).asEagerSingleton()
    //TODO -- verify that PostgresqlRepository is still a singleton
    bind(classOf[RepoHealth]).to(classOf[PostgresqlRepository]).asEagerSingleton()
  }

}

class MetricsModule(environment: Environment, configuration: Configuration) extends AbstractModule {

  def configure() = {
    bind(classOf[Metrics]).to(classOf[FeatureServerMetrics]).asEagerSingleton()
    bind(classOf[Instrumentation]).to(classOf[StdInstrumentation]).asEagerSingleton()
  }

}