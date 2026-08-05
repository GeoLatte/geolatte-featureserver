package integration

import persistence.json.Gen
import persistence.json.Gen._
import play.api.libs.json._
import org.geolatte.geom.Envelope
import java.net.URLEncoder._
import scala.collection.Seq
import scala.util.Try

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 11/22/13
 */
class QueryAPISpec extends InCollectionSpecification {

  def is = s2"""

      The FeatureCollection /query should:
        return 404 when the collection does not exist                              $e1
        return all elements when the collection does exist                         $e2

      The FeatureCollection /list should:
        return the objects contained within the specified bbox as json object      $e3
        reject sql injection in geometry body parameter                            $e3b
        respond to the START query-param                                           $e4
        respond to the LIMIT query-param                                           $e5
        support the SORT parameter                                                 $e5b
        support the SORT-DIRECTION parameter                                       $e5c
        support pagination                                                         $e6

      The FeatureCollection /query should:
        return the objects contained within the specified bbox as a stream          $e7
        support the PROJECTION parameter                                            $e8
        support the SORT parameter                                                  $e8b
        support the SORT-DIRECTION parameter                                        $e8c
        support the QUERY parameter                                                 $e9
        support the WITH-VIEW query-param                                             $e14
        support the WITH-VIEW query-param and a view with no projection clause        $e15
        BAD_REQUEST response code if the PROJECTION parameter is invalid              $e10
        BAD_REQUEST response code if the Query parameter is an invalid expression   $e11

      The FeatureCollection /distinct should:
        return distinct values                                                      $e14b

      General:
        Query parameters should be case insensitive                                 $e12

      The FeatureCollection /query in  CSV should:
        return the objects with all attributes within JSON Object tree              $e13


     Projection may specify fields not in inputJson (works only on postgresql)
        with Json output, fields are set to JsNull                                  $e16
        with CSV output, fields are empty strings                                   $e17

     The QUERY parameter on a JSONB collection should:
        support the IS NULL predicate                                                $e18
        support the IS NOT NULL predicate                                            $e19
        support the regex predicate, honouring backslash escapes                     $e20
        support the JSON-contains predicate                                          $e21
        support to_date comparisons                                                  $e22

     Query values are data, never SQL — a JSONB collection should:
        match a value containing a single quote                                      $e23
        match a value containing a double quote and a backslash                      $e24
        not let a quoted payload in an equality break out of the query               $e25
        not let a quoted payload in a JSON-contains break out of the query           $e26

  """

  //import default values
  import UtilityMethods._

  //Generators for data
  val propertyObjGenerator = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3), "something" -> Gen.oneOf("else", "bad"))
  val nestedPropertyGenerator = propertyObjGenerator.map {
    jsObj => jsObj ++ Json.obj("nestedprop" -> Json.obj("nestedfoo" -> "bar"))
  }
  def lineStringGenerator(mc: String = "") = Gen.lineString(3)(mc)
  val idGen = Gen.id
  def geoJsonFeatureGenerator(mc: String = "") = Gen.geoJsonFeature(idGen, lineStringGenerator(mc), nestedPropertyGenerator)
  def gjFeatureArrayGenerator(mc: String = "", size: Int = 10) = Gen.geoJsonFeatureArray(geoJsonFeatureGenerator(mc), size)

  def e1 = getQuery(testDbName, "nonExistingCollection", "")(contentAsJsonStream).applyMatcher(_.status must equalTo(NOT_FOUND))

  def e2 = {
    val features = gjFeatureArrayGenerator(size = 1).sample.get
    withFeatures(testDbName, testColName, features) {
      getQuery(testDbName, testColName, "")(contentAsJsonStream).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSomeFeatures(features))
      )
    }
  }

  def e3 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeaturesInJson(featuresIn01)))
      )
  }

  def e3b = withTestFeatures(0, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getListBody(testDbName, testColName, "", s"SRID=4326;POINT(10 10)') OR TRUE --").applyMatcher(
        res => res.status must equalTo(BAD_REQUEST))
  }

  def e4 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchTotalInJson(100)))
      )
  }

  def e5 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 10)).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchTotalInJson(100)))
      )
  }

  def e5b = pending

  def e5c = pending

  def e6 = withTestFeatures(100, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val buffer = scala.collection.mutable.ListBuffer[JsValue]()
        for (start <- 0 to 90 by 10) {
          buffer += getList(testDbName, testColName, Map("bbox" -> bbox, "start" -> start, "limit" -> 10)).responseBody.get
        }
        collectFeatures(buffer.toSeq) must beFeatures(featuresIn01)
      }
  }

  def e7 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        getQuery(testDbName, testColName, Map("bbox" -> bbox))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(featuresIn01)
        }
      }
  }

  def e8 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(projectedFeatures)
        }
      }
  }

  def e8b = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val sort = "properties.foo"
        val projectedFeatures = project(projection)(featuresIn01)
        val sortedFeatures = JsArray(projectedFeatures.value.sortBy[String](jsValue => (jsValue \ "properties" \ "foo").as[String]))
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection, "sort" -> sort))(contentAsJsonStream).applyMatcher {
          res =>
            {
              res.responseBody must beSomeFeatures(sortedFeatures, true)
            }
        }
      }
  }

  def e8c = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.num"
        val sort = "properties.foo"
        val sortdir = "DESC"
        val projectedFeatures = project(projection)(featuresIn01)
        val sortedFeatures = JsArray(projectedFeatures.value.sortBy[String](jsValue => (jsValue \ "properties" \ "foo").as[String]).reverse)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection, "sort" -> sort, "sort-direction" -> sortdir))(
          contentAsJsonStream
        ).applyMatcher {
          res =>
            {
              res.responseBody must beSomeFeatures(sortedFeatures, true)
            }
        }
      }
  }

  def e9 = withTestFeatures(100, 200) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val picksFoo = (__ \ "properties" \ "foo").json.pick
        val filteredFeatures = JsArray(
          featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
        )
        val queryObj = "properties.foo='bar1'"
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "query" -> encode(queryObj, "UTF-8")))(contentAsJsonStream) applyMatcher {
          res => res.responseBody must beSomeFeatures(filteredFeatures)
        }
      }
  }

  def e14 = withTestFeatures(100, 200) {
    val projection = "properties.foo,properties.num"
    val jsInViewDef = Json.obj("query" -> JsString("properties.foo = 'bar1'"), "projection" -> Json.arr("properties.foo", "properties.num"))
    loadView(testDbName, testColName, "view-1", jsInViewDef)

    (bbox: String, featuresIn01: JsArray) => {
      val picksFoo = (__ \ "properties" \ "foo").json.pick
      val filteredFeatures = JsArray(
        featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
      )
      val projected = project(projection)(filteredFeatures)
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "with-view" -> "view-1"))(contentAsJsonStream) applyMatcher {
        res => res.responseBody must beSomeFeatures(projected)
      }
    }
  }

  def e14b = withTestFeatures(100, 200) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val picksFoo = (__ \ "properties" \ "foo").json.pick
        val distinctFoo = featuresIn01.value.flatMap(jsv => jsv.asOpt(picksFoo)).map(_.as[String]).distinct.toSeq

        getDistinct(testDbName, testColName, Map("bbox" -> bbox, "projection" -> "properties.foo"))(contentAsJsonStream) applyMatcher { res =>
          val strings = res.responseBody.map(_.value.flatMap(_.as[List[String]])).getOrElse(Nil)
          strings must containTheSameElementsAs(distinctFoo)
        }
      }
  }

  def e15 = withTestFeatures(100, 200) {
    val jsInViewDef = Json.obj("query" -> JsString("properties.foo = 'bar1'"))
    loadView(testDbName, testColName, "view-2", jsInViewDef)

    (bbox: String, featuresIn01: JsArray) => {
      val picksFoo = (__ \ "properties" \ "foo").json.pick
      val filteredFeatures = JsArray(
        featuresIn01.value.filter(jsv => jsv.asOpt(picksFoo) == Some(JsString("bar1")))
      )
      getQuery(testDbName, testColName, Map("bbox" -> bbox, "with-view" -> "view-2"))(contentAsJsonStream) applyMatcher {
        res => res.responseBody must beSomeFeatures(filteredFeatures)
      }
    }
  }

  def e10 = getQuery(testDbName, testColName, Map("projection" -> "fld["))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e11 = getQuery(testDbName, testColName, Map("query" -> """ (foo = 1 """))(contentAsJsonStream).applyMatcher {
    _.status must equalTo(BAD_REQUEST)
  }

  def e12 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val lcResponse = getList(testDbName, testColName, Map("bbox" -> bbox, "limit" -> 5)).responseBody.get
        val ucResponse = getList(testDbName, testColName, Map("BBOX" -> bbox, "LIMIT" -> 5)).responseBody.get
        lcResponse must equalTo(ucResponse)
      }
  }

  def e13 = withTestFeatures(3, 6) {
    (bbox: String, featuresIn01: JsArray) =>
      getQuery(testDbName, testColName, Map("bbox" -> bbox))(contentAsStringStream).applyMatcher(
        res => (res.status must equalTo(OK)) and (res.responseBody must beSome(matchFeaturesInCsv("id,geometry-wkt,foo,nestedprop.nestedfoo,num,something")))
      )
  }

  def e16 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.bar"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsJsonStream).applyMatcher {
          res => res.responseBody must beSomeFeatures(projectedFeatures)
        }
      }
  }

  def e17 = withTestFeatures(10, 10) {
    (bbox: String, featuresIn01: JsArray) =>
      {
        val projection = "properties.foo,properties.bar"
        val projectedFeatures = project(projection)(featuresIn01)
        getQuery(testDbName, testColName, Map("bbox" -> bbox, "projection" -> projection))(contentAsStringStream).applyMatcher {
          res => res.responseBody must beSome(matchFeaturesInCsv("id,geometry-wkt,bar,foo"))
        }
      }
  }

  // The test collection is JSONB-encoded (the create-collection default), so
  // these run through PGJsonpathQueryRenderer. That renderer builds a jsonpath
  // expression which the repository splices into the SQL text as a string
  // literal, so a value crosses two quoting layers on its way to the database.
  // Asserting on the rendered string alone (PGQueryRenderSpec) cannot show that
  // PostgreSQL accepts the result, which is what these exercise.

  def e18 = withFeaturesHavingProperties(Json.obj("code" -> JsNull), Json.obj("code" -> "bla")) {
    features =>
      queryFeatures("properties.code is null") applyMatcher {
        res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
      }
  }

  def e19 = withFeaturesHavingProperties(Json.obj("code" -> JsNull), Json.obj("code" -> "bla")) {
    features =>
      queryFeatures("properties.code is not null") applyMatcher {
        res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value(1))))
      }
  }

  // `\d` has to reach the regex engine as a digit class. Before the values in a
  // jsonpath were escaped, PostgreSQL swallowed the backslash and matched a
  // literal `d` instead — silently, without an error.
  def e20 = withFeaturesHavingProperties(Json.obj("code" -> "bla5bla"), Json.obj("code" -> "blaXbla")) {
    features =>
      queryFeatures("""properties.code ~ /bla\d+bla/""") applyMatcher {
        res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
      }
  }

  def e21 = withFeaturesHavingProperties(
    Json.obj("tags" -> Json.arr("bla", 2)),
    Json.obj("tags" -> Json.arr("blabla"))) {
      features =>
        queryFeatures("""properties.tags @> '["bla"]'""") applyMatcher {
          res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
        }
    }

  def e22 = withFeaturesHavingProperties(
    Json.obj("datum" -> "2019-04-30"),
    Json.obj("datum" -> "2020-01-01")) {
      features =>
        queryFeatures("to_date(properties.datum, 'YYYY-MM-DD') = to_date('2019-04-30', 'YYYY-MM-DD')") applyMatcher {
          res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
        }
    }

  def e23 = withFeaturesHavingProperties(Json.obj("foo" -> "bla'bla"), Json.obj("foo" -> "blabla")) {
    features =>
      queryFeatures("properties.foo = 'bla''bla'") applyMatcher {
        res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
      }
  }

  def e24 = withFeaturesHavingProperties(Json.obj("foo" -> """bla "bla" C:\bla"""), Json.obj("foo" -> "blabla")) {
    features =>
      queryFeatures("""properties.foo = 'bla "bla" C:\bla'""") applyMatcher {
        res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
      }
  }

  // The payload from the reported SQL injection. It has to come back as an
  // ordinary string comparison that matches the feature literally holding it —
  // proof the value never reached SQL statement context.
  def e25 = {
    val payload = """bla'::jsonb OR '1'='1"""
    withFeaturesHavingProperties(Json.obj("foo" -> payload), Json.obj("foo" -> "bla")) {
      features =>
        queryFeatures(s"properties.foo = '${payload.replace("'", "''")}'") applyMatcher {
          res => res.responseBody must beSomeFeatures(JsArray(Seq(features.value.head)))
        }
    }
  }

  // Same payload against `@>`, the operator the report weaponised. Escaped, it
  // is no longer valid JSON, so PostgreSQL rejects the statement and the
  // streaming response fails; unescaped, it made the WHERE clause
  // unconditionally true and streamed back the whole collection. Only that last
  // outcome is a leak, so the assertion is on it rather than on any particular
  // error handling: the request must not succeed with every feature.
  def e26 = withFeaturesHavingProperties(
    Json.obj("tags" -> Json.arr("bla")),
    Json.obj("tags" -> Json.arr("blabla"))) {
      features =>
        val attempt = Try(queryFeatures("""properties.tags @> '1''::jsonb OR ''1''=''1'""").responseBody)
        attempt.toOption.flatten must not(beSomeFeatures(features))
    }

  /**
   * Load features whose properties are exactly the given objects, all inside
   * the same bbox, and hand the loaded array to the block in that order.
   */
  def withFeaturesHavingProperties[T](props: JsObject*)(block: JsArray => T): T = {
    val generated = gjFeatureArrayGenerator("01", props.size).sample.get
    val features = JsArray(generated.value.zip(props).map {
      case (feature, properties) => feature.as[JsObject] ++ Json.obj("properties" -> properties)
    })
    withFeatures(testDbName, testColName, features) {
      block(features)
    }
  }

  def queryFeatures(query: String) =
    getQuery(testDbName, testColName, Map("query" -> encode(query, "UTF-8")))(contentAsJsonStream)

  def withTestFeatures[T](sizeInsideBbox: Int, sizeOutsideBbox: Int)(block: (String, JsArray) => T) = {
    val (featuresIn01, allFeatures) = gjFeatureArrayGenerator("01", sizeInsideBbox)
      .flatMap(f1 => gjFeatureArrayGenerator("1", sizeOutsideBbox).map(f2 => (f1, f2 ++ f1))).sample.get
    val env: Envelope[_] = "01"
    val a = env.toArray()
    val bbox = s"${a(0)},${a(1)},${a(2)},${a(3)}"
    withFeatures(testDbName, testColName, allFeatures) {
      block(bbox, featuresIn01)
    }
  }

  //hardcoded projection parameter for now
  def project(projection: String)(features: JsArray) = {
    import play.api.libs.functional.syntax._
    import play.api.libs.functional._

    val fields = List("foo", "num", "something", "nestedprop")
    val projectionFields = projection.split(",").map(fp => fp.split("\\.")(1))
    val fieldsToPrune = fields.filterNot(f => projectionFields.contains(f))
    val fieldsToAdd = projectionFields.filterNot(f => fields.contains(f))

    val pruner: Reads[JsObject] = {

      val p1 = fieldsToPrune.foldLeft((__ \ "properties" \ "id").json.prune) {
        (p, field) => p andThen ((__ \ "properties" \ field).json.prune)
      }

      fieldsToAdd.foldLeft(p1) {
        (p, field) => p andThen (__ \ "properties").json.update((__ \ "bar").json.put(JsNull))
      }

    }

    JsArray(
      for {
        f <- features.value
        pruned = f.transform(pruner)
      } yield pruned.asOpt.get
    )
  }

  def collectFeatures(listOfResponses: Seq[JsValue]) =
    listOfResponses.foldLeft(JsArray())((state, elem) => (elem \ "features").asOpt[JsArray] match {
      case Some(arr) => state ++ arr
      case _         => state
    })

}
