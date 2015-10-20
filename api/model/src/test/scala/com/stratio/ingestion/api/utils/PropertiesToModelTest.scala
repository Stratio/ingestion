package com.stratio.ingestion.api.utils

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Created by eruiz on 19/10/15.
 */

@RunWith(classOf[JUnitRunner])
class PropertiesToModelTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {

  describe("The Json Parser") {
    it("should parse this sink from the json") {
      Given("a json sink")

      val v = PropertiesToModel.propertiesToModel("/new.properties")
      println(v)
      //        val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq())
      //
      //        val agent = Agent(source,Seq(),Seq())
      //        val result = ModelToProperties.modelToProperties(agent)
      //
      //        result should be (PropertiesToModel.propertiesToModel("/new.properties"))

    }


  }
}


