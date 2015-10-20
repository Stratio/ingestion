/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.ingestion.api.utils

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.Agent
import com.stratio.ingestion.api.model.sink.AgentSink
import JsonParser._
import com.stratio.ingestion.api.model.source.AgentSource
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import spray.json._

/**
 * Created by eruiz on 15/10/15.
 */

@RunWith(classOf[JUnitRunner])
class JsonParserTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {

  describe("The Json Parser") {
    it("should parse this sink from the json") {
      Given("a json sink")

      val sink = AgentSink("1", "typo", "mongo", "mongoSink", Seq(), Seq())
      sink should be(sink.toJson.convertTo[AgentSink])

      val channel = AgentChannel("1", "typo", "mongo", "mongoSink", Seq(), Seq())
      val sink2 = AgentSink("1", "typo", "mongo", "mongoSink", Seq(), Seq(channel))
      println(sink2)
      println(sink2.toJson)
      sink2 should be(sink2.toJson.convertTo[AgentSink])


      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq())

      val agent = Agent(source,Seq(),Seq())
      agent should be (agent.toJson.convertTo[Agent])
    }


  }
}
