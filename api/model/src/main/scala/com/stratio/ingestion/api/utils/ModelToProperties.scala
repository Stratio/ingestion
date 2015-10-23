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

import java.io.{File, PrintWriter}

import com.stratio.ingestion.api.model.commons.Agent

/**
 * Created by eruiz on 16/10/15.
 */

object ModelToProperties {
  //TODO Put this val in a configuration file.
  val sourceName = "sources"
  val interceptorName = "interceptors"
  val channelName = "channels"
  val sinkName = "sinks"
  val unionName = "union"
  val typeName = "type"

  def modelToProperties(agent: Agent): Unit = {

    val pw = new PrintWriter(new File("src/test/resources/new.properties"))
    writeComponents(agent, pw)
    writeSource(agent, pw)
    writeChannel(agent, pw)
    writeSink(agent, pw)
    writeUnion(agent, pw)

    pw.close
  }

  def writeComponents(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("#Name the components on this agent" + "\n\n")

    pw.write(agent.id + "." + sourceName + " = " + agent.source.id + "\n")
    pw.write(agent.id + "." + channelName + " = " + agent.channels.map(chan => chan.id).reduce(_ + " " + _) + "\n")
    pw.write(agent.id + "." + sinkName + " = " + agent.sinks.map(sink => sink.id).reduce(_ + " " + _) + "\n")
  }

  def writeSource(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + sourceName.toUpperCase + " #####" + "\n\n")
    pw.write(agent.id + "." + sourceName + "." + agent.source.id + "." + typeName + " = " + agent.source._type + "\n")
    agent.source.settings.foreach(settings => pw.write(agent.id + "." + sourceName + "." + agent.source.id + "." +
      settings.id + " = " + settings.value + "\n"))
    pw.write("\n\n##### " + interceptorName.toUpperCase + " #####" + "\n\n")
    //TODO We need define the interceptors behaviour

    agent.source.interceptors.foreach(interceptor => pw.write(agent.id + "." + sourceName + "." + agent.source.id + "." +
      interceptor + " = " + interceptor + "\n"))
  }

  def writeChannel(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + channelName.toUpperCase + " #####" + "\n\n")

    agent.channels.foreach(channel => pw.write(agent.id + "." + channelName + "." + channel.id + "." + typeName + " = " +
      channel._type + "\n"))

    agent.channels.foreach(channel => channel.settings.foreach(settings => pw.write(agent.id + "." + channelName + "." +
      channel.id + "." + settings.id + " = " + settings.value + "\n")))

  }

  def writeSink(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + sinkName.toUpperCase + " #####" + "\n\n")

    agent.sinks.foreach(sink => pw.write(agent.id + "." + sinkName + "." + sink.id + "." + typeName + " = " +
      sink._type + "\n"))

    agent.sinks.foreach(sink => sink.settings.foreach(settings => pw.write(agent.id + "." + sinkName + "." +
      sink.id + "." + settings.id + " = " + settings.value + "\n")))

  }

  def writeUnion(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + unionName.toUpperCase + " #####" + "\n\n")

    pw.write(agent.id + "." + sourceName + "." + agent.source.id + "." + channelName + " = " + agent.channels.map
      (chan => chan.id).reduce(_ + " " + _) + "\n")

    agent.sinks.foreach(sink => pw.write(agent.id + "." + sinkName + "." + sink.id + "." + "channel" + " = " + sink.channels.id + "\n"))

  }


}
