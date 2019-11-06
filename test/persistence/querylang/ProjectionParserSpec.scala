package persistence.querylang

import org.specs2.mutable.Specification
import play.api.libs.json.Reads._
import play.api.libs.json._

/**
 * Created by Karel Maesen, Geovise BVBA on 30/03/17.
 */
class ProjectionParserSpec extends Specification {

  "The ProjectionParser.mkProjection( List[JsPath]) " should {

    val jsonText =
      """
        |{"id":"123747","geometry":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"type":"Feature","properties":{"entity":{"id":123747.0,"langsGewestweg":true,"ident8":"R0010751","opschrift":0.0,"afstand":5.0,"zijdeVanDeRijweg":"RECHTS","locatie":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"binaireData":{"kaartvoorstellingkleingeselecteerd":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAANUlEQVR42mNggIH//32AuAGIA4CYhQFJYjUQ/0fCqyEKIDr+Y8EBDFCjsEk2ENCJ1048rgUADfltxmBALPMAAAAASUVORK5CYII=","md5":"NdkrvxQrcszB7vQWjcd/Kg=="},"kaartvoorstellingklein":{"type":"BinaryProperty","properties":{"breedte":"7","hoogte":"7"},"mime":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAcAAAAHCAYAAADEUlfTAAAAMklEQVR42mNgQAAfIG4A4gAgZkESZ1gNxP+R8GqYAh80CRgGmQA2CptkA0GdeO3E6VoA+T0cGBVUSPoAAAAASUVORK5CYII=","md5":"WuQL2tBI1Wslm3+8GwyzAw=="}}}},"_bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735]}
      """.stripMargin

    val projectedJsonText =
      """
        |{"geometry":{"type":"Point","crs":{"properties":{"name":"EPSG:31370"},"type":"name"},"bbox":[153278.02886708,208573.85006735,153278.02886708,208573.85006735],"coordinates":[153278.02886708,208573.85006735]},"type":"Feature","properties":{"entity":{"id" : 123747.0, "ident8":"R0010751"}}}
      """.stripMargin

    val jsObj = Json.parse(jsonText).as[JsObject]

    val pathList: List[JsPath] = List(__ \ "geometry", __ \ "type", __ \ "properties" \ "entity" \ "id", __ \ "properties" \ "entity" \ "ident8")

    "create a Reads[Object] that strips all non-mentioned properties from the json" in {

      val projection = ProjectionParser.mkProjection(pathList).get

      val obj = jsObj.as(projection)

      obj must_== Json.parse(projectedJsonText).as[JsObject]

    }

    "be robust w.r.t. to redundant path elements " in {
      val redundantPaths = __ \ "properties" \ "entity" \ "ident8" :: pathList

      val projection = ProjectionParser.mkProjection(redundantPaths).get

      val obj = jsObj.as(projection)

      obj must_== Json.parse(projectedJsonText).as[JsObject]

    }

    "Projection puts JsNull for non-existent (empty) paths" in {

      val withNonExistentPath = pathList :+ (__ \ "properties" \ "entity" \ "doesntexist")

      val projection = ProjectionParser.mkProjection(withNonExistentPath).get

      val obj = jsObj.asOpt(projection)

      val expected = Some(Json.obj("properties" ->
        Json.obj("entity" ->
          Json.obj("doesntexist" -> JsNull))).deepMerge(Json.parse(projectedJsonText).as[JsObject]))

      obj must_== expected

    }

  }

  "ProjectionParser  " should {

    val jsonText =
      """
          | { "a1" : 1, "a2" : { "b1" : [ { "ca" : 2, "cb" : 3}, { "ca" : 4, "cb" : 5 }  ], "b2" : 7 }, "a3" : 8 }
        """.stripMargin

    val json = Json.parse(jsonText).as[JsObject]

    " succesfully parse projection expression: a1, a2.b1[ca] " in {
      val projection = ProjectionParser.parse("a1, a2.b1[ca]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj("a1" -> 1, "a2" -> Json.obj("b1" -> Json.arr(Json.obj("ca" -> 2), Json.obj("ca" -> 4))))

      obj must_== expected
    }

    " return empty array for arrays that don't exist: a2.doesnotexist[prop1, prop2] " in {
      val projection = ProjectionParser.parse("a2.doesnotexist[prop1, prop2]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj("a2" ->
        Json.obj(
          "doesnotexist" -> JsArray()
        ))

      obj must_== expected
    }

    " return empty array for arrays that don't exist: doesnotexist[prop1, nested[prop2]] " in {
      val projection = ProjectionParser.parse("doesnotexist[prop1, nested[prop2]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "doesnotexist" -> JsArray()
      )

      obj must_== expected
    }

    " succesfully parse projection expression: a1, a2.b1[ca, cb] and combine the elements in b1  " in {
      val projection = ProjectionParser.parse("a1, a2.b1[ca, cb]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "a1" -> 1,
        "a2" -> Json.obj(
          "b1" -> Json.arr(
            Json.obj(
              "ca" -> 2,
              "cb" -> 3
            ), Json.obj(
              "ca" -> 4,
              "cb" -> 5
            )
          )
        )
      )

      obj must_== expected
    }

    " ignore whitespace in parse expressions " in {
      val projection = ProjectionParser.parse("a1         ,    a2.b1[ca]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj("a1" -> 1, "a2" -> Json.obj("b1" -> Json.arr(Json.obj("ca" -> 2), Json.obj("ca" -> 4))))

      obj must_== expected
    }

    " throw an expression when given invalid parse expression " in {
      ProjectionParser.parse("a/b/c").get must throwA[QueryParserException]
    }

  }

  "ProjectionParser (nested missing arrays) " should {

    val jsonText =
      """
          | {
          |   "items": [{
          |     "key": "value1",
          |     "data" : []
          |   },{
          |     "key": "value2",
          |     "data" : [{
          |       "key2" : "value3"
          |     }]
          |   }]
          | }
        """.stripMargin

    val json = Json.parse(jsonText).as[JsObject]

    " ignore further inner expression when a top level array does not exist: data[prop1, prop2, nested[prop3, nested2[prop4]]] " in {
      val projection = ProjectionParser.parse("data[prop1, prop2, nested[prop3, nested2[prop4]]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "data" -> JsArray()
      )

      obj must_== expected
    }

    " return empty arrays when inner array does not exist within top level array: items[key, nested[prop3, nested2[prop4]]] " in {
      val projection = ProjectionParser.parse("items[key, nested[prop3, nested2[prop4]]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "items" -> Seq(
          Json.obj(
            "key" -> "value1",
            "nested" -> JsArray()
          ),
          Json.obj(
            "key" -> "value2",
            "nested" -> JsArray()
          )
        )
      )

      obj must_== expected

    }

    " handle empty arrays correctly: items[data[key2, key3, nested[key4]]] " in {
      val projection = ProjectionParser.parse("items[data[key2, key3, nested[key4]]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "items" -> Seq(
          Json.obj(
            "data" -> JsArray()
          ),
          Json.obj(
            "data" -> Seq(
              Json.obj(
                "key2" -> "value3",
                "key3" -> JsNull,
                "nested" -> JsArray()
              )
            )
          )
        )
      )

      obj must_== expected

    }

  }

  "ProjectionParser (complex json object with nested arrays)  " should {

    val jsonText =
      """
          | {
          |  "id": "166613",
          |  "geometry": {
          |    "type": "Point",
          |    "crs": {
          |      "properties": {
          |        "name": "EPSG:31370"
          |      },
          |      "type": "name"
          |    },
          |    "bbox": [
          |      152447.11,
          |      225707.33,
          |      152447.11,
          |      225707.33
          |    ],
          |    "coordinates": [
          |      152447.11,
          |      225707.33
          |    ]
          |  },
          |  "type": "Feature",
          |  "properties": {
          |    "id": 166613,
          |    "externalId": "{F551E466-2FF7-40A9-A5D6-7D2D6F52A903}",
          |    "aanzichten": [
          |      {
          |        "id": 186274,
          |        "clientId": "aanzicht1",
          |        "hoek": 0.479634450540162,
          |        "type": "ZELFDE_ALS_OPSTELLING",
          |        "borden": [
          |          {
          |            "id": 265015,
          |            "clientId": "bord1",
          |            "externalId": "{7C2D4E75-379E-4CB4-A7E0-D6132D8ABFC6}",
          |            "type": "5110d9ec236f6700ba9904b4",
          |            "bibBordId": "511394ed236f6700ba9a1e0b",
          |            "code": "F37",
          |            "x": 0,
          |            "y": 900,
          |            "breedte": 1200,
          |            "hoogte": 150,
          |            "folieType": "1",
          |            "leverancierItem": {
          |              "key": 772,
          |              "naam": "Janssens",
          |              "actief": true
          |            },
          |            "fabricageJaar": 1950,
          |            "fabricageMaand": 1,
          |            "fabricageType": {
          |              "key": 1,
          |              "naam": "Standaardbestek 250",
          |              "actief": true
          |            },
          |            "bevestigdMetBandit": false,
          |            "vorm": "rh",
          |            "parameters": [
          |              "sy-S117",
          |              "De Bosduif"
          |            ],
          |            "actief": true,
          |            "bevestigingsProfielen": [
          |              {
          |                "id": 351843,
          |                "bevestigingen": [
          |                  {
          |                    "id": 387430,
          |                    "type": {
          |                      "key": 1,
          |                      "naam": "Steun",
          |                      "actief": true
          |                    },
          |                    "ophangingId": "steun1"
          |                  },
          |                  {
          |                    "id": 387431,
          |                    "type": {
          |                      "key": 1,
          |                      "naam": "Steun",
          |                      "actief": true
          |                    },
          |                    "ophangingId": "steun2"
          |                  }
          |                ]
          |              }
          |            ],
          |            "datumPlaatsing": "01/01/1950",
          |            "simadCategory": -2,
          |            "beugels": [
          |
          |            ],
          |            "overruleBeugels": true
          |          }
          |        ],
          |        "wijzigingsTimestamp": 1360058860858,
          |        "anker": {
          |          "type": "Point",
          |          "crs": {
          |            "properties": {
          |              "name": "EPSG:31370"
          |            },
          |            "type": "name"
          |          },
          |          "bbox": [
          |            152451.292915152,
          |            225710.78091017,
          |            152451.292915152,
          |            225710.78091017
          |          ],
          |          "coordinates": [
          |            152451.292915152,
          |            225710.78091017
          |          ]
          |        },
          |        "voorstellingWidth": 1200,
          |        "voorstellingHeight": 180,
          |        "fotos": [
          |          {
          |            "id": 69924,
          |            "objectid": "5110d9ec236f6700ba9904b2"
          |          }
          |        ]
          |      }
          |    ],
          |    "ophangingen": [
          |      {
          |        "id": 118192,
          |        "clientId": "steun1",
          |        "x": 170,
          |        "diameter": 76,
          |        "lengte": 1700,
          |        "ondergrondType": {
          |          "key": 4,
          |          "naam": "Niet gespecifieerd",
          |          "actief": true
          |        },
          |        "sokkelAfmetingen": {
          |          "key": 1,
          |          "naam": "300x300x600, LG-51/VG-51/VG-76",
          |          "actief": true,
          |          "hoogte": 600,
          |          "breedte": 300,
          |          "diepte": 300
          |        },
          |        "kleur": {
          |          "key": 1,
          |          "naam": "Grijs (Standaardbestek 250)",
          |          "actief": true,
          |          "rgb": "rgb(64,69,69)"
          |        },
          |        "datumPlaatsing": "01/01/1950",
          |        "fabricageType": "SB250",
          |        "conformSB250": true
          |      },
          |      {
          |        "id": 118193,
          |        "clientId": "steun2",
          |        "x": 1030,
          |        "diameter": 76,
          |        "lengte": 1700,
          |        "ondergrondType": {
          |          "key": 4,
          |          "naam": "Niet gespecifieerd",
          |          "actief": true
          |        },
          |        "sokkelAfmetingen": {
          |          "key": 1,
          |          "naam": "300x300x600, LG-51/VG-51/VG-76",
          |          "actief": true,
          |          "hoogte": 600,
          |          "breedte": 300,
          |          "diepte": 300
          |        },
          |        "kleur": {
          |          "key": 1,
          |          "naam": "Grijs (Standaardbestek 250)",
          |          "actief": true,
          |          "rgb": "rgb(64,69,69)"
          |        },
          |        "datumPlaatsing": "01/01/1950",
          |        "fabricageType": "SB250",
          |        "conformSB250": true
          |      }
          |    ],
          |    "langsGewestweg": true,
          |    "ident8": "N0110001",
          |    "opschrift": 9.3,
          |    "afstand": -49,
          |    "positie": 9.25,
          |    "afstandInZijstraat": 0,
          |    "zijdeVanDeRijweg": "RECHTS",
          |    "locatie": {
          |      "type": "Point",
          |      "crs": {
          |        "properties": {
          |          "name": "EPSG:31370"
          |        },
          |        "type": "name"
          |      },
          |      "bbox": [
          |        152447.11,
          |        225707.33,
          |        152447.11,
          |        225707.33
          |      ],
          |      "coordinates": [
          |        152447.11,
          |        225707.33
          |      ]
          |    },
          |    "editable": false,
          |    "beginDatum": "01/01/1950",
          |    "wijzigingsDatum": "05/02/2013",
          |    "gemeente": "Kapellen",
          |    "simadCategory": -1,
          |    "schaalRef": 0.01,
          |    "schaalOpst": 0.01,
          |    "status": "ACTUEEL",
          |    "conformSB250": true,
          |    "uuid": "13ef4cd2-26c2-48d7-9bb4-274ce5998bbe"
          |  }
          |}
        """.stripMargin

    val json = Json.parse(jsonText).as[JsObject]

    " succesfully parse simple projection expression " in {
      val projection = ProjectionParser.parse("properties.id").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613
        )
      )

      obj must_== expected
    }

    " succesfully parse multiple simple projection expression " in {
      val projection = ProjectionParser.parse("properties.id,  properties.zijdeVanDeRijweg,  properties.ident8").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "zijdeVanDeRijweg" -> "RECHTS",
          "ident8" -> "N0110001"
        )
      )

      obj must_== expected
    }

    " succesfully parse a mix of simple projection expressions and array expressions " in {
      val projection = ProjectionParser.parse("properties.id, properties.ophangingen[clientId, diameter, lengte]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "ophangingen" -> Seq(
            Json.obj(
              "clientId" -> "steun1",
              "diameter" -> 76,
              "lengte" -> 1700
            ),
            Json.obj(
              "clientId" -> "steun2",
              "diameter" -> 76,
              "lengte" -> 1700
            )
          )
        )
      )

      obj must_== expected
    }

    " succesfully parse nested array expressions " in {
      val projection = ProjectionParser.parse("properties.id, properties.aanzichten[hoek, borden[code, clientId]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "aanzichten" -> Seq(
            Json.obj(
              "hoek" -> 0.479634450540162,
              "borden" -> Seq(
                Json.obj(
                  "code" -> "F37",
                  "clientId" -> "bord1"
                )
              )
            )
          )
        )
      )

      obj must_== expected
    }

    " succesfully parse simple expressions within array expressions" in {
      val projection = ProjectionParser.parse("properties.id, properties.ophangingen[kleur.naam]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "ophangingen" -> Seq(
            Json.obj(
              "kleur" -> Json.obj(
                "naam" -> "Grijs (Standaardbestek 250)"
              )
            ),
            Json.obj(
              "kleur" -> Json.obj(
                "naam" -> "Grijs (Standaardbestek 250)"
              )
            )
          )
        )
      )

      obj must_== expected
    }

    " return empty array if arrays don't exist " in {
      val projection = ProjectionParser.parse("properties.id, properties.zijdes[kleur]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "zijdes" -> JsArray()
        )
      )

      obj must_== expected
    }

    " succesfully parse multiple nested array expressions " in {
      val projection = ProjectionParser.parse("properties.id, properties.aanzichten[borden[bevestigingsProfielen[bevestigingen[id, type.naam]]]]").get

      val obj = json.as(projection.reads)

      val expected = Json.obj(
        "properties" -> Json.obj(
          "id" -> 166613,
          "aanzichten" -> Seq(
            Json.obj(
              "borden" -> Seq(
                Json.obj(
                  "bevestigingsProfielen" -> Seq(
                    Json.obj(
                      "bevestigingen" -> Seq(
                        Json.obj(
                          "id" -> 387430,
                          "type" -> Json.obj(
                            "naam" -> "Steun"
                          )
                        ),
                        Json.obj(
                          "id" -> 387431,
                          "type" -> Json.obj(
                            "naam" -> "Steun"
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )

      obj must_== expected
    }

  }

}
