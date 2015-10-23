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

import java.util.Properties

import com.jayway.jsonpath.JsonPath
import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{Agent, Attribute}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource
import net.minidev.json.JSONArray

import scala.io.Source
import scala.util.Try

/**
 * Created by eruiz on 19/10/15.
 */
object PropertiesToModel {

  def propertiesToModel(nameFile: String) = {
    val prop = new Properties()
    prop.load(prop.getClass.getResourceAsStream(nameFile))
    val agentName = prop.keySet().toArray()(0).asInstanceOf[String].split('.')(0)


    val idSource = prop.getProperty(agentName + "." + Constants.sourceName)
    val typeSource = prop.getProperty(agentName + "." + Constants.sourceName + "." + idSource + "." + Constants.typeName)
    val settingsSourceBeforeFilter: Seq[Attribute] = getSettings(prop, idSource, Constants.sourceName, typeSource)
    val settingsSource: Seq[Attribute] = settingsSourceBeforeFilter.filter(att => att._type != Constants.channelName && att._type != Constants.typeName)

    //TODO Add union in channels and sinks
    //    val union = settingsSourceBeforeFilter.map(att => att._type=="channels")

    val idChannels = prop.getProperty(agentName + "." + Constants.channelName).split(" ")
    val typeChannels = idChannels.map(id => (id, prop.getProperty(agentName + "." + Constants.channelName + "." + id + "." +
      Constants.typeName)))
    val settingsChannel: Seq[Attribute] = typeChannels.map(idChan => (idChan._1, getSettings(prop, idChan._1, Constants.channelName,
      idChan._2))).flatMap(setting => setting._2)


    val idSinks = prop.getProperty(agentName + "." + Constants.sinkName).split(" ")
    val typeSinks = idSinks.map(id => (id, prop.getProperty(agentName + "." + Constants.sinkName + "." + id + "." + Constants.typeName)))
    val settingsSinks: Seq[Attribute] = typeSinks.map(idSink => (idSink._1, getSettings(prop, idSink._1, Constants.sinkName, idSink
      ._2))).flatMap(setting => setting._2)

    //    val union = settingsSinksBeforeFilter.map(x => x.filter(x => x._type== channelName))


    //TODO Find name in json
    val source = AgentSource(idSource, typeSource, "", Seq(), settingsSource)
    val channels = typeChannels.indices.foldLeft(Seq.empty[AgentChannel]) {
      case (channels, i) =>
//        channels :+ AgentChannel(idChannels(i), typeChannels(i)._2, "", settingsChannel.filter(set => set.id == idChannels(i)), source);
        channels :+ AgentChannel(idChannels(i), typeChannels(i)._2, "", settingsChannel.toList, source);

    }
    //    val channels: Seq[AgentChannel[String]] =
    //      for {
    //        id <- idChannels
    //        typeChannel <- typeChannels
    //        setting <- settingsChannel
    //      } yield AgentChannel(id, typeChannel._2, "", setting, source)
    val sinks = typeSinks.indices.foldLeft(Seq.empty[AgentSink]) {
      case (sinks, i) =>
//        sinks :+ AgentSink(idSinks(i), typeSinks(i)._2, "", settingsSinks.filter(set => set.id == idSinks(i)), channels(i))
        sinks :+ AgentSink(idSinks(i), typeSinks(i)._2, "", settingsSinks.toList, channels(i))
    }
    Agent(agentName, source, channels, sinks)
  }

  def getSettings(p: Properties, id: String, component: String, _type: String): Seq[Attribute] = {
    val agentName = p.keySet().toArray()(0).asInstanceOf[String].split('.')(0)
    val enuKeys = p.keys()
    var attribute: Seq[Attribute] = Seq.empty[Attribute]
    while (enuKeys.hasMoreElements) {
      val key = enuKeys.nextElement().toString
      if (key.startsWith(agentName + "." + component + "." + id) &&
        !key.startsWith(agentName + "." + component + "." + id + "." + _type) &&
        !key.startsWith(agentName + "." + component + "." + id + "." + "channel")
      ) {
        val typeNotType = key.split(agentName + "." + component + "." + id + ".")(1)
        if (typeNotType != "type") {
          //          attribute :+= Attribute(id, typeNotType, "", getRequired(p, component, _type, typeNotType).getOrElse(false)
          //            , p.getProperty(key))
          attribute :+= Attribute(typeNotType, "Boolean", "", getRequired(p, component, _type, typeNotType).getOrElse
            (false), p.getProperty(key))
        }
      }
    }
    attribute
  }

  def getRequired(p: Properties, component: String, _type: String, setting: String): Option[Boolean] = {
    val componentInSingular = component.slice(0, component.length - 1)
    val file = Source.fromFile("src/main/resources/" + componentInSingular + "/" + _type + "/" + _type + componentInSingular.capitalize + ".json").mkString
    val js = JsonPath.parse(file)
    Try {
      js.read("$.descriptors.components..settings.." + setting + "..required").asInstanceOf[JSONArray].get(0).asInstanceOf[Boolean]
    }.toOption
  }
}
