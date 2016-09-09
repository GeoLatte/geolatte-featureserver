package filters

import javax.inject.Inject

import kamon.Kamon
import metrics.KamonUserMetrics$
import featureserver.Global
import play.api.http.HttpFilters
import play.api.mvc.Filter
import play.filters.gzip.GzipFilter

class Filters @Inject() (gzipFilter: GzipFilter) extends HttpFilters {
  def filters = Seq(gzipFilter, Global.loggingFilter)
}