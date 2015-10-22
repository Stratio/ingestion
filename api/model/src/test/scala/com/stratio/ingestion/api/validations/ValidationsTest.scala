package com.stratio.ingestion.api.validations

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{Agent, Attribute}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource
import com.stratio.ingestion.api.utils.ModelToProperties
import com.stratio.ingestion.api.model.validations
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

/**
 * Created by miguelsegura on 21/10/15.
 */
@RunWith(classOf[JUnitRunner])
class ValidationsTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {

  describe("The Properties Parser") {
    it("should parse this model to properties file") {
      Given("an agent")

      val attribute1 = Attribute("id", "spoolDir", "spooldDir", "spoolDirAttributeDescription", true, "data/spoolDir")
      val attribute2 = Attribute("id", "spoolDir", "fileHeader", "Whether to add a header storing the absolute path " +
        "filename.", false, "FALSE")
      val source = AgentSource("ID", "spoolDir", "src", "SourceDescription", Seq(), Seq(attribute1, attribute2))
      //      val source2 = Source("ID2", "memory", "src2", "SourceDescription2", Seq(), Seq())
      val attributeChannel = Attribute("id", "capacity", "capacity", "The maximum number of events stored in the channel"
        , false, "100")
      val channel = AgentChannel("1", "memory", "mongoChannel", "mongoChannelDescription", Seq(attributeChannel), Seq(source))
      val channel2 = AgentChannel("2", "file", "decisionChannel", "decisionChannelDescription", Seq(attributeChannel), Seq
        (source))
      val attributeSink = Attribute("id", "mongoUri", "mongoUri", "", true, "mongodb://127.0.0.1:27017/example.example")
      val attributeSink2 = Attribute("id", "mappingFile", "mappingFile", "", true, "conf/mongo_schema.json")
      val attributeSink3 = Attribute("id", "dynamic", "dynamic", "", true, "false")
//      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
//        (attributeSink, attributeSink2, attributeSink3), Seq(channel))
      val sink = AgentSink("1", "com.stratio.ingestion.sink.mongodb.MongoSink", "mongoSink", "mongoSinkDescription", Seq
          (attributeSink, attributeSink2, attributeSink3), Seq.empty[AgentChannel])
      val agent = Agent("id", source, Seq(channel, channel2), Seq(sink))

//      ModelToProperties.modelToProperties(agent)
      validations.errors.giveMeAgent(agent)

      println(validations.errors.listMsg)
    }

  }
}
