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
import com.stratio.ingestion.api.model.commons.{Attribute, Agent}
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

//      val attribute1 = Attribute("id", "spoolDir", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
//      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
//        "filename.", false, "FALSE")
//      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
//      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
//      val attributeChannel = Attribute("id", "capacity", "capacity", "The maximum number of events stored in the channel"
//        , false, "100")
//      val channel = AgentChannel("1", "memory", "mongoChannel", "mongoChannelDescription", Seq(attributeChannel), Seq(source))
//      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), Seq
//        (source))
//      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
//      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
//      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
//      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
//        (attributeSink, attributeSink2, attributeSink3), Seq(channel))
//      val agent = Agent(source, Seq(channel, channel2), Seq(sink))
//
//      ModelToProperties.modelToProperties(agent)


    }

  }
}