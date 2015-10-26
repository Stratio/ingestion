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
import com.stratio.ingestion.api.model.commons.{Agent, Attribute}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource
import com.stratio.ingestion.api.utils.JsonParser._
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


      val attribute1 = Attribute( "spooldDir", "spoolDir","", true, "data/spoolDir")
      val attribute2 = Attribute( "fileHeader", "spoolDir","", false, "FALSE")
      val source = AgentSource("src", "spoolDir", "", Seq(), Seq(attribute1, attribute2))
      val attributeChannel = Attribute("capacity", "capacity", "", false, "100")
      val channel = AgentChannel("mongoChannel", "memory", "", Seq(attributeChannel), source)
      val channel2 = AgentChannel("decisionChannel", "file", "", Seq(attributeChannel), source)
      val attributeSink = Attribute("mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("mongoSink", "com.stratio.ingestion.sink.mongodb.MongoSink", "", Seq
        (attributeSink, attributeSink2, attributeSink3), channel)
      sink should be(sink.toJson.convertTo[AgentSink])
      val sink2 = AgentSink("1", "typo", "mongo", Seq.empty[Attribute],channel)
      channel2 should be(channel2.toJson.convertTo[AgentChannel])
      val agent = Agent("a1",source,Seq(channel),Seq(sink,sink2))
      

      Given("a json sink")

      agent should be (agent.toJson.convertTo[Agent])

    }


  }
}
