/**
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner


/**
 * Created by eruiz on 16/10/15.
 */
@RunWith(classOf[JUnitRunner])
class ModelToPropertiesTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {

  describe("The Properties Parser") {
    it("should parse this model to properties file") {
      Given("an agent")

      val attribute1 = Attribute("fileHeader", "string", "File header", false, "FALSE")

      val attribute2 = Attribute("spoolDir", "string", "Spooling directory", true, "data/spoolDir")
      // val source = AgentSource("src", "spoolDir", "", Seq(), Seq(attribute1, attribute2))
      val source1 = AgentSource("src", "spoolDir", "Spooling Directory Source", Seq(), Seq(attribute1))
      val source2 = AgentSource("src", "spoolDir", "Spooling Directory Source", Seq(), Seq(attribute1,attribute2))
      val attributeChannel = Attribute("capacity", "integer", "Capacity", false, "100")
      val attributeChannel1 = Attribute("capacity", "integer", "Capacity", false, "10000")
      val channel = AgentChannel("mongoChannel", "memory", "Memory Channel", Seq(attributeChannel), source1)
      val channel1 = AgentChannel("mongoChannel", "memory", "Memory Channel", Seq(attributeChannel), source2)
      val channel2 = AgentChannel("decisionChannel", "file", "File Channel", Seq(attributeChannel1), source2)
      val attributeSink = Attribute("mongoUri", "string", "Mongo URI", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("dynamic", "boolean", "Dynamic", true, "false")
      val attributeSink3 = Attribute("mappingFile", "string", "Mapping file", true, "conf/mongo_schema.json")

      val sink = AgentSink("mongoSink", "mongodb", "MongoDB Sink", Seq
        (attributeSink, attributeSink2, attributeSink3), channel)
      val sink2 = AgentSink("mongoSink", "mongodb", "MongoDB Sink", Seq
        (attributeSink, attributeSink2, attributeSink3), channel1)

      val agent1 = Agent("a", source1, Seq(channel), Seq(sink))
      val agent2 = Agent("a", source2, Seq(channel1,channel2), Seq(sink2))


      val properties=ModelToProperties.modelToProperties(agent2)





    }
  }
}