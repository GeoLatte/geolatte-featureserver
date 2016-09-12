package featureserver.mongodb

import akka.util.ByteString
import featureserver.FeatureWriter
import org.specs2.mutable.Specification
import play.api.libs.iteratee._

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration.Duration
import play.api.libs.json.JsObject
import featureserver.json.Gen
import org.geolatte.geom.Envelope
import utilities.ReactiveGeoJson

import scala.collection._

/**
 * @author Karel Maesen, Geovise BVBA
 *         creation-date: 8/2/13
 */
class ReactiveGeoJsonSpecs extends Specification {

  import scala.language.reflectiveCalls
  import scala.concurrent.ExecutionContext.Implicits.global

  "The reactive GeoJsonTransformer" should {

    "read valid input and transform it to a stream of GeoJson Features" in {

      val testSize = 500
      val (_, enumerator) = testEnumerator(testSize)
      val sink = new mutable.ArrayBuffer[JsObject]()
      val fw = dummyWriter(sink)
      val future = (enumerator andThen Enumerator.eof) |>>> ReactiveGeoJson.mkStreamingIteratee(fw, "\n")
      //Wait until de iteratee is done
      val stateIteratee = Await.result(future, Duration(5000, "millis"))
      //Wait until de Iteratee is finished writing to featurewriter
      Await.result(stateIteratee.right.get, Duration(5000, "millis"))
      val result = sink.toList

      (stateIteratee must beRight) and
        (result must not be empty) and
        (result must beLike {
          case l: List[_] if l.size == testSize => ok
          case _ => ko(": second result element is not of correct size")
        })

    }

    "Correctly handle very large inputs" in {
      val inpJsonStr =
        """{"id":"30","geometry":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[155829.446519351,197295.77189979,155829.446519351,197295.77189979],"coordinates":[155829.446519351,197295.77189979]},"type":"Feature","properties":{"entity":{"id":30,"externalId":"{0B2BC9AF-6C2E-413C-8874-DDE9EC9B5EA4}","aanzichten":[{"id":41,"clientId":"aanzicht1","hoek":3.36026159946463,"type":"ZELFDE_ALS_OPSTELLING","borden":[{"id":47,"clientId":"bord2","externalId":"{3FDF072B-2D7A-4EED-BBD1-F970F7A4CFDD}","afbeelding":"eNqtkktv00AQx+9IfAdz4SGcdmef3hyQ4thuCyEtaR7AJWpKCIHECXZMCgyflYeQeF0REhIHdu00MSK9VbPe9Xg8v/nP7grsHgfDdDyKna7YoXj1ChCkgEdZlqTGo8ZTOJjFfSJ1//wrIwgMez2nCc4dx0+yNB1OdmvxYjmcJ8MYiTUXVqswM8sHJ1wxBFEFXaUSKSEGRDVBw0LDFaYa2jQpSRFl9sGIgWPdM+NOp9bFpICvjQs7qFEPHsHleIGjZJZLYfiWRUFEFPUrNFC1Cg/DoOL7AVQirUikarweBcG7oj4YBQS1vsz6BZkZMzMrkSlGVP9Dpvo/8rlZ5mByki1LTI4ij10SEax848TIPM/VKn+VxY92W2xrgPk5J3PTnPSAu5RpvgIWJ15waJ4slHYVLXNgzVndG+RSc1dJugXCTH+nyJRyiShD2AYym02RmYt1AYGjNARgTLpabkco04dwFXC2Jb8oy0C7AOV0vk5vZdN0Ya+25woD2cIo6m7RsIHQizTYQ603/Eat08vXsIMgFYD0NtG91mHYNGs3bLVRCloK9Q7aRXqzbvOUoiBs0Kg6GZw+GT4dPRs/fzGZxrP5yyRdZK+WZ6/f1Px6EEZ7+wd37zXuNw+PHrSO251u7+GjxwQo40IqT3/69rWy4+zeuP3z5q3vVbf/8cPnP79+v7/25foPw/8L2EhcvU==","type":"4fe43343cfeadac3530a6600","bibBordId":"50bda790e4b06692f6ad1902","code":"F31","x":0,"y":2760,"breedte":2000.0,"hoogte":300.0,"folieType":"2","fabricageJaar":0,"fabricageMaand":0,"fabricageType":{"key":1,"naam":"Standaardbestek 250","actief":true},"bevestigdMetBandit":false,"beheerder":{"key":26,"naam":"112 - Puurs","actief":true,"type":"Agentschap Wegen en Verkeer","kleur":"#8B978F","gebiednummer":112,"gebiedcode":"1MD8EA","gebiednummernaam":"112 - Puurs","gebiednaam":"AWV112"},"externeBeheerderPresent":false,"vorm":"wwr","parameters":["Antwerpen"],"actief":true,"bevestigingsProfielen":[{"id":68,"bevestigingen":[{"id":67,"type":{"key":1,"naam":"Steun","actief":true},"ophangingId":"steun1"}]}],"datumPlaatsing":"01/01/1950","simadCategory":-2,"beugels":[],"overruleBeugels":false},{"id":48,"clientId":"bord1","externalId":"{1A9E25C3-C0A7-4294-82DF-11D62AF7382B}","afbeelding":"eNqtkktv00AQx+9IfAdz4SGcdmf25c0ByXm1hZCWNA/gEjUlhEDiBDsmBYbPykNIvK4ICYkDu3aaBJHeqlnvejz+/2Z2ZyV1jiuDZDSMvI7cQbp6BRgh0FGaxon10Hqa+tOox5TpnX/ljIBTt+s1wLvjleI0SQbj3TCaLwazeBARc+bDcpV25tkQTGhOIItgiqgIGbMgNIwsiyxX2mzkZEqxPMrdQzUOnnPPrDuZOJfiHL4yId1AWz0EjBajOQ3jaVYKX5LBshkZc/lkbs3OfIOMVEPzDxnNf+Rzc8z++CRdZMy3EJoqyjIvlFmoCwKNKARYqRUAKgrDmuYBlt7lmQXJjHAZeV3v3SatExEPAt/o7FXlP7rDcwcAlPU5ntkjUAEIH7kRS2De8ZyDmVhq42vc5MCKs7w3JJQRvla4BcLt/k6Ja+0zuQnha8h0OiFuL9YFBEHKEoBz5Ru1HaHtPqSvQfAt+jwtB+MDbMrFSt5MJ8ncXe3AlxayhZHn3VLDGoIX1eCaWq6X6mG7m63VNoHSACpYR/eah9WGXTvVZouUxI1Q96CVyxtlp9MaQbqgreqkf/pk8HT4bPT8xXgSTWcv42SevlqcvX4TlsqVam1v/+Duvfr9xuHRg+Zxq93pPnz0mAFyIZUOzKdvXws73u6N2z9v3vpe9HsfP3z+8+v3+2tfrv+w/L80MVw3","type":"4fe43343cfeadac3530a6602","bibBordId":"50bda790e4b06692f6ad18fd","code":"F29","x":0,"y":2430,"breedte":2000.0,"hoogte":300.0,"folieType":"2","fabricageJaar":0,"fabricageMaand":0,"fabricageType":{"key":1,"naam":"Standaardbestek 250","actief":true},"bevestigdMetBandit":false,"beheerder":{"key":26,"naam":"112 - Puurs","actief":true,"type":"Agentschap Wegen en Verkeer","kleur":"#8B978F","gebiednummer":112,"gebiedcode":"1MD8EA","gebiednummernaam":"112 - Puurs","gebiednaam":"AWV112"},"externeBeheerderPresent":false,"vorm":"wwr","parameters":["Boom","7"],"actief":true,"bevestigingsProfielen":[{"id":69,"bevestigingen":[{"id":68,"type":{"key":1,"naam":"Steun","actief":true},"ophangingId":"steun1"}]}],"datumPlaatsing":"01/01/1950","simadCategory":-2,"beugels":[],"overruleBeugels":false},{"id":49,"clientId":"bord3","externalId":"{DFFE6F31-3E25-49B6-BB5B-9CBD7C164863}","afbeelding":"eNqtkktv00AQx+9IfAdz4SE27c4+vTkgxYnTFkJa0jyAS9SUEAKJE+yYFBg+Kw8h8boiJCQO7NppYkR6q8be9Xj0/83szErsHteGyXgUeV25w/DqFaDIAI/SNE6sx6yncTCL+lSZ/vlfThE49npeE7w7XhCnSTKc7FaixXI4j4cRUmcEVru0K88eQYXmCLIMpswUMkotiBmKloWWK202dDKlaB7l7sU6B8+5Z9adTp2LcQ5fm5DuYbZ68CkuxwscxbOsFL4ig2VTNObyydyaXXmBzLDOzD9kZv4jn5tjDiYn6bLAFCiz2GUQ39bq9VDZs5V4yGRJmECVgkAGJVMNaroKSviKv3Ozd4e0kgi57xOjs0+V41zzXAMAsznHc9sC5YMgjBuxSptPPOewTCy1IZoVObDmrO4NCmUE0YptgXDbhVPkWhMqixC+gcxmU+T2Yl1AEKgsAThXxKjtCG3PIYkGwbfo87QcDAEoysVa3kqnycJdbZ9IC9nCyPNuqWEDYRfV4EZfbQSNSqeX7WEHQWkA5W+ie63DsGn3bthqo5KsEOodtHN5s+p0WjOQLmirOhmcPhk+HT0bP38xmUaz+cs4WaSvlmev31SCai2s7+0f3L3XuN88PHrQOm53ur2Hjx5TYFxIpX3z6dvX0o63e+P2z5u3vpdJ/+OHz39+/X5/7cv1H5b/F+jvXHn=","type":"4fe43343cfeadac3530a6604","bibBordId":"50bda790e4b06692f6ad18fd","code":"F29","x":0,"y":2100,"breedte":2000.0,"hoogte":300.0,"folieType":"2","fabricageJaar":0,"fabricageMaand":0,"fabricageType":{"key":1,"naam":"Standaardbestek 250","actief":true},"bevestigdMetBandit":false,"beheerder":{"key":26,"naam":"112 - Puurs","actief":true,"type":"Agentschap Wegen en Verkeer","kleur":"#8B978F","gebiednummer":112,"gebiedcode":"1MD8EA","gebiednummernaam":"112 - Puurs","gebiednaam":"AWV112"},"externeBeheerderPresent":false,"vorm":"wwr","parameters":["Rumst","2"],"actief":true,"bevestigingsProfielen":[{"id":70,"bevestigingen":[{"id":69,"type":{"key":1,"naam":"Steun","actief":true},"ophangingId":"steun1"}]}],"datumPlaatsing":"01/01/1950","simadCategory":-2,"beugels":[],"overruleBeugels":false},{"id":50,"clientId":"bord4","externalId":"{48195298-FA33-45D7-B45D-09045AEF9B6F}","afbeelding":"eNqtkktv00AQx+9IfIdw4SGcdmef3hyQ8nJbCGlJ8wAuUVNCCCROsGNSYPisPITE64qQkDgwa6eJEemtmvWux+P5/Wd2V2H3uDaMx6Ow0FU7HK9eAYYc8ChJopg8Tp7BwSzsM237518FQxDY6xWaULhTqERJHA8nu+VwsRzOo2GIzJkHq1XRLNIhmTQCQZXAlrhGzhiBuGVILCSuIjV0aVqzLCrcg4GAgnPPyJ1OnYtRBl+bVG5wqh58hsvxAkfRLC1FrMhAbIbWXib5rfTBKm79YlAWoihVzRQrNBeZZVKV64Gt6OBdpi/IaBY5fY4Bt//oc/uf/rk55cHkJFnmepKo0tglEcE1SU6Iwvc9a9JXnf3oNs9tAGB6ztGctkD7ID0urFwBsxPPODxNVsZ6huc5sOas7g1KbaVnNN8CEdTfKQpjPKbyELGBzGZTFHSxLiBI1EQAIbRn9XaEoT6UZ0CKLfmZrADrAeTT5Tq9lUzjhbvavqcIsoWR6W6pYQPhF9XgDrXaqDTKnV661jsI2gBofxPdax3Wm7R26602asVzod5BO0tvVl2eMRyUC1JVJ4PTJ8Ono2fj5y8m03A2fxnFi+TV8uz1m3KlWqsHe/sHd+817jcPjx60jtudbu/ho8cMuJBKG99++va1uFPYvXH7581b30te/+OHz39+/X5/7cv1H8T/CznhXED=","type":"4fe43343cfeadac3530a6606","bibBordId":"50bda790e4b06692f6ad1902","code":"F31","x":0,"y":3090,"breedte":2000.0,"hoogte":300.0,"folieType":"2","fabricageJaar":0,"fabricageMaand":0,"fabricageType":{"key":1,"naam":"Standaardbestek 250","actief":true},"bevestigdMetBandit":false,"beheerder":{"key":26,"naam":"112 - Puurs","actief":true,"type":"Agentschap Wegen en Verkeer","kleur":"#8B978F","gebiednummer":112,"gebiedcode":"1MD8EA","gebiednummernaam":"112 - Puurs","gebiednaam":"AWV112"},"externeBeheerderPresent":false,"vorm":"wwr","parameters":["Brussel"],"actief":true,"bevestigingsProfielen":[{"id":71,"bevestigingen":[{"id":70,"type":{"key":1,"naam":"Steun","actief":true},"ophangingId":"steun1"}]}],"datumPlaatsing":"01/01/1950","simadCategory":-2,"beugels":[],"overruleBeugels":false}],"wijzigingsTimestamp":1340355395452,"anker":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[155829.446519351,197295.77189979,155829.446519351,197295.77189979],"coordinates":[155829.446519351,197295.77189979]},"voorstellingWidth":2000,"voorstellingHeight":1320,"fotos":[],"binaireData":{"platgeslagenvoorstelling":{"type":"BinaryProperty","properties":{"breedte":"133","hoogte":"88"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAIYAAABYCAYAAAAwTY/OAAAARUlEQVR42u3BgQAAAADDoPlTX+EAVQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACvAbiYAAHhcSmyAAAAAElFTkSuQmCC","md5":"fsEvTaG1vNX+GMRp8JAquQ=="},"platgeslagenvoorstellingklein":{"type":"BinaryProperty","properties":{"breedte":"80","hoogte":"53"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAFAAAAA1CAYAAADWKGxEAAAAJklEQVR42u3BAQEAAACCIP+vbkhAAQAAAAAAAAAAAAAAAAAAAHBnQnUAAU/hJMgAAAAASUVORK5CYII=","md5":"nFS4ch0cS/S5tXjzI/YsMQ=="},"platgeslagenvoorstellinggeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"133","hoogte":"88"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAIYAAABYCAYAAAAwTY/OAAAAvElEQVR42u3SwQkAMAwDsey/tEt/hZQMEHTgBYyqkpi1OcFmGNINDIEhMASGwBAYAkNgCAyBITAEhsAAQ2AIDIEhMASGwBAYAkNgCAyBITDAEBgCQ2AIDIEhMASGwBAYAkNgCAwJDIEhMASGwBAYAkNgCAyBITAEhsAAQ2AIDIEhMASGwBAYAkNgCAyBITDAEBgCQ2AIDIEhMASGwBAYAkNgCAwwBIbAEBgCQ2AIDIEhMASGNsIwe+cE++0AKCguXzE6FuQAAAAASUVORK5CYII=","md5":"WIRXdulDvlYjNyvcfEhoiw=="},"platgeslagenvoorstellingkleingeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"80","hoogte":"53"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAFAAAAA1CAYAAADWKGxEAAAAaElEQVR42u3bwQkAQAgDQftvOtZwnCDILKSB+acqiX0MwhSg3gIIECBAgAABAgQIECBAgAABCiBAgAAFECBAgAIIECBAAQQIEKAAAgQIUAABAgQogAABAhRAgAABAgQIEOAhQHP531gDymAGTygsqqsAAAAASUVORK5CYII=","md5":"/Pfzq4s1wOO9Io+kNej6Ug=="}}}],"ophangingen":[{"id":36,"clientId":"steun1","x":1000,"diameter":133,"lengte":4120,"ondergrondType":{"key":3,"naam":"Verhard oppervlak","actief":true},"sokkelAfmetingen":{"key":1,"naam":"300x300x600, LG-51/VG-51/VG-76","actief":true,"hoogte":600,"breedte":300,"diepte":300},"kleur":{"key":1,"naam":"Grijs (Standaardbestek 250)","actief":true,"rgb":"rgb(64,69,69)"},"datumPlaatsing":"01/01/1950","fabricageType":"SB250","voetstukFabricageType":"SB250"}],"langsGewestweg":true,"ident8":"N0019031","hm":0.0,"afstand":9,"positie":0.016,"afstandInZijstraat":0.0,"zijdeVanDeRijweg":"RECHTS","beheerder":{"key":26,"naam":"112 - Puurs","actief":true,"type":"Agentschap Wegen en Verkeer","kleur":"#8B978F","gebiednummer":112,"gebiedcode":"1MD8EA","gebiednummernaam":"112 - Puurs","gebiednaam":"AWV112"},"locatie":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[155829.446519351,197295.77189979,155829.446519351,197295.77189979],"coordinates":[155829.446519351,197295.77189979]},"editable":false,"beginDatum":"01/01/1950","simadCategory":-1,"schaalRef":0.02,"schaalOpst":0.02,"binaireData":{"kaartvoorstellingkleingeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAANUlEQVR42mNggIH//32AuAGIA4CYhQFJYjUQ/0fCqyEKIDr+Y8EBDFCjsEk2ENCJ1048rgUADfltxmBALPMAAAAASUVORK5CYII=","md5":"NdkrvxQrcszB7vQWjcd/Kg=="},"kaartvoorstelling":{"type":"BinaryProperty","properties":{"breedte":"19","hoogte":"19"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAABMAAAATCAYAAAByUDbMAAAAnklEQVR42mNgGAUEAAcQuwJxJxCbkaqZCYiNgbgCiPcA8Xcg/g/FHcQYoATE6UC8GojfImkG4b9AfAZqEFaXCQNxGBDPAuJ7aJr/Q8VmAnEoVC0GMIf6+ywQ/0PT/BOND3IhCz6vdCIpBoXDbiAuB+ICLC4D4QB8hplD/e0CjSEYaMBhWAM5Ue9DjsvwgdWkhhkxLmyAuohlNK+OFAAAxW88ADKkX0oAAAAASUVORK5CYII=","md5":"yILB3rzq+AgSTNH6io8k/A=="},"kaartvoorstellinggeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"19","hoogte":"19"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAABMAAAATCAYAAAByUDbMAAAApUlEQVR42mNgGAV4wf//HEDsCsSdQGxGqmYmIDYG4gog3gPE34H4PxR3EGOAEhCnA/FqIH6LpBmE/wLxGbBBWF32/78wEIcB8Swgvoem+T9UbCYQh4LVYjHAHOrvs0D8D03zTzQ+yIUs+LzSiaQYFA67gbgciAuwuAyEA/AZZg71tws4hhDiDTgMayAn6n1Idxl+A1eTFmbEubAB7CKKDBoFQwsAAA8J7U4KWH01AAAAAElFTkSuQmCC","md5":"10AUzjqMOmj8WSgj24CjgA=="},"kaartvoorstellingklein":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAAMklEQVR42mNgQAAfIG4A4gAgZkESZ1gNxP+R8GqYAh80CRgGmQA2CptkA0GdeO3E6VoA+T0cGBVUSPoAAAAASUVORK5CYII=","md5":"WuQL2tBI1Wslm3+8GwyzAw=="}}}}}

        """
      val batched = inpJsonStr.getBytes("UTF-8").grouped(7736).toList.map(ByteString(_))
      val sink = new mutable.ArrayBuffer[JsObject]()
      val fw = dummyWriter(sink)
      val enumerator = Enumerator(batched: _*)
      val future = (enumerator andThen Enumerator.eof) |>>> ReactiveGeoJson.mkStreamingIteratee(fw, "\n")
      //Wait until de iteratee is done
      val stateIteratee = Await.result(future, Duration(5000, "millis"))
      //Wait until de Iteratee is finished writing to featurewriter
      Await.result(stateIteratee.right.get, Duration(5000, "millis"))
      val result = sink.toList

      (stateIteratee must beRight) and
        (result must not be empty) and
        (result must beLike {
          case l: List[_] if l.size == 1 => ok
          case _ => ko(": second result element is not of correct size")
        })

    }
  }

  def dummyWriter(sink: mutable.ArrayBuffer[JsObject]) = new FeatureWriter {
    def add(objects: Seq[JsObject]) = { sink ++= objects; Future { objects.size } }
    def updateIndex() = Future.successful(true)
  }

  def genFeatures(size: Int) = {
    implicit val extent = new Envelope(0, 0, 90, 90)
    val fGen = Gen.geoJsonFeature(Gen.id, Gen.lineString(3), Gen.properties("foo" -> Gen.oneOf("boo", "bar")))
    for (i <- 0 until size) yield fGen.sample
  }

  def testEnumerator(size: Int, batchSize: Int = 1000) = {
    val text = (genFeatures(size).map(g => g.get) mkString "\n") + "\n"
    val batched = text.getBytes("UTF-8").grouped(batchSize).toList.map(ByteString(_))
    (text, Enumerator(batched: _*))
  }

}
