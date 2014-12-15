package integration

import config.ConfigurationValues
import nosql.json.Gen
import org.specs2.matcher.MatchResult
import play.api.libs.json._
import play.api.http.Status._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/5/13
 */
class TransactionAPISpec  extends InCollectionSpecification {


  //TODO add additional spec tests for consistency of count metadata field on insert
  //TODO --

  def is = s2"""


                                                                                  ${section("mongodb", "postgresql")}
     The Transaction /upsert should:
       return 404 when the collection does not exist                              $e1
       return OK when the collection does exist, and data is valid                $e2
       insert value idem-potently when object does not exist                      $e3
       update value idem-potently when object does already exist                  $e4

     The transaction /insert should:
        return OK when the collection exists, and data is valid                   $e5
        metadata query returns the inserted number of objects                     $e6

     The transactions /delete should:
        deleting an element and return status DELETED                             $e7

                                                                                  ${section("mongodb", "postgresql")}

  """


  //import default values
  import RestApiDriver._
  import UtilityMethods._
  import Gen._

 //Generators for data
  val prop = Gen.properties("foo" -> Gen.oneOf("bar1", "bar2", "bar3"), "num" -> Gen.oneOf(1, 2, 3))
  def geom(mc: String = "") = Gen.lineString(3)(mc)
  val idGen = Gen.id
  def feature(mc: String = "") = Gen.geoJsonFeature(idGen, geom(mc), prop)
  def featureArray(mc: String = "", size: Int = 10) = Gen.geoJsonFeatureArray(feature(mc), size)

  def e1 = {
    val f = feature().sample.get
    postUpsert(testDbName, "NonExistingCollection", f) applyMatcher { _.status must equalTo(NOT_FOUND) }
  }

  def e2 = {
      val f = feature().sample.get
      postUpsert(testDbName,testColName, f) applyMatcher  { res =>
        res.status must equalTo(OK)
      }
  }

  def e3 = {
    val f = feature().sample.get
    removeData(testDbName, testColName)
    postUpsert(testDbName, testColName, f)
    postUpsert(testDbName, testColName, f)
    getList(testDbName, testColName, "") applyMatcher { res =>
      res.responseBody must beSome(matchFeaturesInJson(Json.arr(f)))
    }
  }

  def e4 = {
    val f = feature().sample.get
    removeData(testDbName, testColName)
    postUpsert(testDbName, testColName, f)
    val modifier = __.json.update(( __ \ "properties" \ "num").json.put(JsNumber(4)))
    val modifiedFeature = f.transform(modifier).asOpt.get
    postUpsert(testDbName, testColName, modifiedFeature)
    postUpsert(testDbName, testColName, modifiedFeature)
    getList(testDbName, testColName, "") applyMatcher { res =>
      res.responseBody must beSome(matchFeaturesInJson(Json.arr(modifiedFeature)))
    }
  }

  def e5 = {
    val fs = featureArray(size=100).sample.get
    removeData(testDbName, testColName)
    val data = fs.value map (j => Json.stringify(j)) mkString ConfigurationValues.jsonSeparator getBytes ("UTF-8")
    val res1 = loadData(testDbName, testColName, data)
    val res2 = getList(testDbName, testColName, "")

    res1.applyMatcher{
      _.status must equalTo(OK)
    } and res2.applyMatcher{
      res => res.responseBody must beSome( matchFeaturesInJson(fs))
    }
  }

  def e6 = pending

  def e7 = pending

}