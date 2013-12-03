package integration

import org.specs2._
import org.specs2.mutable.Around
import org.specs2.specification.{Step, Fragments, Scope}
import org.specs2.execute.{Result, AsResult}
import play.api.test.{Helpers, WithApplication, FakeApplication}
import play.api.Play
import integration.RestApiDriver._
import play.api.test.FakeApplication

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 12/3/13
 */
abstract class NoSqlSpecification(app: FakeApplication = FakeApplication()) extends Specification {
  val testDbName = "xfstestdb"
  val testColName = "xfstestcoll"

  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
       fs ^
       Step(Play.stop())

}

abstract class InDatabaseSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import RestApiDriver._
  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
      Step(makeDatabase(testDbName)) ^
      fs ^
      Step(dropDatabase(testDbName)) ^
      Step(Play.stop())
}

abstract class InCollectionSpecification(app: FakeApplication = FakeApplication()) extends NoSqlSpecification {
  import RestApiDriver._
  override def map(fs: =>Fragments) = Step(Play.start(app)) ^
    Step(makeDatabase(testDbName)) ^
    Step(makeCollection(testDbName, testColName)) ^
    fs ^
    Step(dropDatabase(testDbName)) ^
    Step(Play.stop())
}


