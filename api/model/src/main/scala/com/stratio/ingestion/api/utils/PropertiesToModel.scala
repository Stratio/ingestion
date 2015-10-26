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

import java.util.Properties

import com.stratio.ingestion.api.model.channel.AgentChannel
import com.stratio.ingestion.api.model.commons.{Agent, Attribute}
import com.stratio.ingestion.api.model.sink.AgentSink
import com.stratio.ingestion.api.model.source.AgentSource

/**
 * Created by eruiz on 19/10/15.
 */
object PropertiesToModel {
  val agentName="a1"

  def propertiesToModel(nameFile: String) = {
    val p = new Properties()
    p.load(p.getClass().getResourceAsStream(nameFile))
    //    p.list(System.out)
    val nameSource = p.getProperty(agentName + ".sources")
    val typoSource = p.getProperty(agentName + ".sources." + nameSource + ".type")
    val settingsSource = getSettings(p, nameSource)

    val nameChannels = p.getProperty(agentName + ".channels")
    val typoChannels = p.getProperty(agentName + ".channels." + nameChannels + ".type")
    val settingsChannel = getSettings(p, nameChannels)

    val nameSinks = p.getProperty(agentName + ".sinks")
    val typoSink = p.getProperty(agentName + ".sinks." + nameSinks + ".type")
    val settingsSink = getSettings(p, nameSinks)




//    val source = AgentSource("idSource", typoSource, nameSource, "description", Seq(), settingsSource)
//    val channels = Seq(AgentChannel("idChan", typoChannels, nameChannels, "description", settingsChannel, Seq(source)))
//    val sinks = Seq(AgentSink("idSink", typoSink, nameSinks, "description", settingsSink, channels))


//    Agent(source, channels, sinks)

  }

  def getSettings(p: Properties, name: String) = {
    val enuKeys = p.keys()
    var valor = 0
    var value: Seq[String] = Seq.empty[String]
    var attribute: Seq[Attribute] = Seq.empty[Attribute]
    while (enuKeys.hasMoreElements) {
      val key = enuKeys.nextElement().toString
      if (key.startsWith(agentName + ".sources." + name)) {
        valor = valor + 1
        value :+= p.getProperty(key)
        attribute :+= Attribute("idAttribute", key.split(agentName + ".sources.src.")(1), "", "", true, p.getProperty
        (key))
      }
    }
    attribute
  }

}
