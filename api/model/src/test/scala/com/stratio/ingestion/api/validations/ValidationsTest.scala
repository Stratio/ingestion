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
package com.stratio.ingestion.api.validations

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{Agent, Attribute}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource
import com.stratio.ingestion.api.model.validations

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 21/10/15.
 */
@RunWith(classOf[JUnitRunner])
class ValidationsTest extends FunSpec
with GivenWhenThen
with ShouldMatchers
with BeforeAndAfterAll {


  override def beforeAll() {

  }

  describe("The Properties Parser") {
    it("check sink's channels are empty") {
      Given("an agent")

      val attribute1 = Attribute("id", "spoolDir", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
        "filename.", false, "FALSE")
      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("id", "capacity", "capacity", "The maximum number of events stored in the channel"
        , false, "100")
      val channel = AgentChannel("1", "memory", "mongoChannel", "mongoChannelDescription", Seq(attributeChannel), source)
      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), source)
      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
          (attributeSink, attributeSink2, attributeSink3), null)
      val agent = Agent("id", source, Seq(channel, channel2), Seq(sink))

      var listMsg = ListBuffer.empty[String]
      listMsg = validations.connectionErrors.noChannels(agent)
//      listMsg = validations.connectionErrors.notConnectedThatExists(agent)
      assert(listMsg.length > 0)
//      println(validations.connectionErrors.listMsg)
    }

    it("check sink has channels that exists") {
      Given("an agent")

      val attribute1 = Attribute("id", "spoolDir", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
        "filename.", false, "FALSE")
      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("id", "capacity", "capacity", "The maximum number of events stored in the channel"
        , false, "100")
      val channel = AgentChannel("1", "memory", "mongoChannel", "mongoChannelDescription", Seq(attributeChannel), source)
      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), source)
      val channel3 = AgentChannel("3", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel)
        , source)
      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
        (attributeSink, attributeSink2, attributeSink3), channel3)
      val sink2 = AgentSink("2", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription",
        Seq
        (attributeSink, attributeSink2, attributeSink3), channel)
      val agent = Agent("id", source, Seq(channel, channel2), Seq(sink, sink2))

      var listMsg = ListBuffer.empty[String]
      listMsg = validations.connectionErrors.notConnectedThatExists(agent)
      assert(listMsg.length > 0)
      println(validations.connectionErrors.listMsg)
    }

    it("check a required attribute is empty") {
      Given("an agent")

      val attribute1 = Attribute("id", "spoolDir", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
        "filename.", false, "FALSE")
      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("id", "capacity", "capacity", "The maximum number of events stored in the channel"
        , false, "100")
      val channel = AgentChannel("1", "memory", "mongoChannel", "mongoChannelDescription", Seq(attributeChannel), source)
      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), source)
      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "")
      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
        (attributeSink, attributeSink2, attributeSink3), channel)
      val agent = Agent("id", source, Seq(channel, channel2), Seq(sink))

      var listMsg = ListBuffer.empty[String]
      listMsg = validations.valueErrors.settingFailure(agent)
      assert(listMsg.length > 0)
      println(validations.valueErrors.listMsg)
    }

    it("check composition errors") {
      Given("an agent")

      val attribute1 = Attribute("id", "", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
        "filename.", false, "FALSE")
      val source = AgentSource("", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("id", "", "capacity", "The maximum number of events stored in the channel"
        , false, "100")
      val channel = AgentChannel("", "", "mongoChannel", "mongoChannelDescription", Seq(), source)
      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), source)
      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "")
      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
      val sink = AgentSink("", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription",
        Seq(attributeSink, attributeSink2, attributeSink3), channel)
      val agent = Agent("id", source, Seq(channel, channel2), Seq(sink))
      val agent2 = Agent("id", null, Seq(), Seq())

      var listMsg = ListBuffer.empty[String]
      listMsg = validations.compositionErrors.badComponents(agent2)
      listMsg = validations.compositionErrors.badSettings(agent)
      listMsg = validations.compositionErrors.badEntity(agent)

      assert(listMsg.length > 0)
            println(validations.connectionErrors.listMsg)
    }


  }
}
