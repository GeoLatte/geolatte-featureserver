package integration

import java.net.URLEncoder.encode

import persistence.Repository
import persistence.postgresql.PostgresqlRepository
import play.api.libs.json._
import slick.jdbc.PostgresProfile.api._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * Registering an existing table is the one route by which a column name
 * travels from a request body into the text of every later query on that
 * collection. These run that route against a real table.
 */
class RegisterCollectionAPISpec extends InDatabaseSpecification {

  def is = s2"""

      Registering an existing table should:
        return CREATED for a table with a geometry column                                   $e1
        serve a query against the registered table                                          $e2
        serve a query when the column was registered in a different case                    $e3

      A geometry column is a name, never SQL — a registered table should:
        not run a payload that closes the identifier                                        $e4

    """

  import UtilityMethods._
  import API._

  def e1 = createTableAndRegister("regcoll", "geom").status must equalTo(CREATED)

  // The registered table is served through PGRegularQueryRenderer, and its
  // geometry column and primary key are now quoted identifiers. Only a round
  // trip shows that the quoted forms still address the columns; a rendered
  // string proves nothing about what PostgreSQL accepts.
  def e2 = {
    insertRow("regcoll", "first")
    queryLabels("regcoll", "label = 'first'") must beSome(Seq("first"))
  }

  // PostgreSQL folds an unquoted identifier, so registering `GEOM` for a column
  // named `geom` used to work. Quoting the name would have ended that if it
  // were not lower-cased first.
  def e3 = {
    createTableAndRegister("casedcoll", "geom", registerAs = Some("GEOM"))
    insertRow("casedcoll", "second")
    queryLabels("casedcoll", "label = 'second'") must beSome(Seq("second"))
  }

  // The payload closes the ST_AsEWKT call, adds a column of its own, and
  // reopens the call so the statement stays valid — unquoted it therefore ran,
  // and every feature came back carrying `leaked`. Quoted it is a single column
  // name, no such column exists, and the query yields nothing. The assertion is
  // on the leak rather than on any particular failure: what must never happen
  // is that the subselect is answered.
  def e4 = {
    val payload = """geom), (select current_setting('is_superuser')) as leaked, ST_AsEWKT(geom"""
    createTableAndRegister("injectcoll", "geom", registerAs = Some(payload))
    insertRow("injectcoll", "third")
    queryValuesOf("leaked", "injectcoll", "label = 'third'") must beEmpty
  }

  private lazy val repository =
    currentApp.injector.instanceOf[Repository].asInstanceOf[PostgresqlRepository]

  private def runSql(statement: DBIO[Int]): Unit =
    Await.result(repository.database.run(statement), 10.seconds)

  /**
   * Create a plain PostGIS table — the kind this server does not own and can
   * only register — and offer it to the register endpoint.
   */
  private def createTableAndRegister(collection: String, geometryColumn: String, registerAs: Option[String] = None) = {
    runSql(
      sqlu"""create table #${quoted(testDbName)}.#${quoted(collection)} (
               id serial primary key,
               #${quoted(geometryColumn)} geometry(Point, 31370),
               label varchar(255)
             )"""
    )
    val body = Json.obj(
      "collection" -> collection,
      "geometry-column" -> registerAs.getOrElse[String](geometryColumn),
      "extent" -> Json.obj(
        "crs" -> 31370,
        "envelope" -> Json.arr(0.0, 0.0, 1000000.0, 1000000.0)
      )
    )
    FakeRequestResult.POSTJson(DATABASES / testDbName / "register", body, contentAsJson)
  }

  private def insertRow(collection: String, label: String): Unit =
    runSql(
      sqlu"""insert into #${quoted(testDbName)}.#${quoted(collection)} (geom, label)
             values (st_setsrid(st_makepoint(1, 1), 31370), $label)"""
    )

  private def queryLabels(collection: String, query: String): Option[Seq[String]] =
    queryFor(collection, query, "label").map(_.map(_.as[String]))

  /** Empty when the query does not come back at all, which is a non-leak too. */
  private def queryValuesOf(field: String, collection: String, query: String): Seq[JsValue] =
    Try(queryFor(collection, query, field)).toOption.flatten.getOrElse(Nil)

  private def queryFor(collection: String, query: String, field: String): Option[Seq[JsValue]] =
    getQuery(testDbName, collection, Map("query" -> encode(query, "UTF-8")))(contentAsJsonStream)
      .responseBody
      .map(js => (js \\ field).toSeq)

  private def quoted(name: String): String = s""""$name""""

}
