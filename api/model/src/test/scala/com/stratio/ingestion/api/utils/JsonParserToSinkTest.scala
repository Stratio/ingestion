package com.stratio.ingestion.api.utils


import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner


/**
 * Created by eruiz on 15/10/15.
 */

@RunWith(classOf[JUnitRunner])
class JsonParserToSinkTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {


  describe("The Json Parser") {
    it("should parse this sink frome the json") {
      Given("a json sink")
      val jsonResponse ="""{"id":1,"typo":"String","name":"mongo","description":"mongoSink"}"""
      val sink = JsonParserToSink.parse(jsonResponse)
      Then("we should ")
      sink.id should be("1")

    }


  }
}
