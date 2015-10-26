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

      val attribute1 = Attribute( "spooldDir", "spoolDir","", true, "data/spoolDir")
      val attribute2 = Attribute( "fileHeader", "spoolDir","", false, "FALSE")
      val source = AgentSource("src", "spoolDir", "", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("src2", "memory", "", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("capacity", "capacity", "", false, "100")
      val attributeChannel1 = Attribute("capacity", "capacity", "", false, "10000")
      val channel = AgentChannel("mongoChannel", "memory", "", Seq(attributeChannel), source)
      val channel2 = AgentChannel("decisionChannel", "file", "", Seq(attributeChannel1), source)
      val attributeSink = Attribute("mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("mongoSink", "com.stratio.ingestion.sink.mongodb.MongoSink", "", Seq
        (attributeSink, attributeSink2, attributeSink3), channel)
      val agent = Agent("a", source, Seq(channel, channel2), Seq(sink))

      ModelToProperties.modelToProperties(agent)



    }
  }
}