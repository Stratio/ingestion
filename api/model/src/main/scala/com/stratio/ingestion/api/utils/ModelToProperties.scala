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

import java.io.{File, PrintWriter}

import com.stratio.ingestion.api.model.commons.Agent

/**
 * Created by eruiz on 16/10/15.
 */

object ModelToProperties{

  def modelToProperties(agent: Agent): File = {
  val file =new File("src/test/resources/new.properties")
    val pw = new PrintWriter(file)
    writeComponents(agent, pw)
    writeSource(agent, pw)
    writeChannel(agent, pw)
    writeSink(agent, pw)
    writeUnion(agent, pw)

    pw.close
    file
  }

  def writeComponents(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("#Name the components on this agent" + "\n\n")

    pw.write(agent.id + "." + Constants.sourceName + " = " + agent.source.id + "\n")
    pw.write(agent.id + "." + Constants.channelName + " = " + agent.channels.map(chan => chan.id).reduce(_ + " " + _) + "\n")
    pw.write(agent.id + "." + Constants.sinkName + " = " + agent.sinks.map(sink => sink.id).reduce(_ + " " + _) + "\n")
  }

  def writeSource(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + Constants.sourceName.toUpperCase + " #####" + "\n\n")
    pw.write(agent.id + "." + Constants.sourceName + "." + agent.source.id + "." + Constants.typeName + " = " + agent.source._type + "\n")
    agent.source.settings.foreach(settings => pw.write(agent.id + "." + Constants.sourceName + "." + agent.source.id + "." +
      settings.id + " = " + settings.value + "\n"))
    pw.write("\n\n##### " + Constants.interceptorName.toUpperCase + " #####" + "\n\n")
    //TODO We need define the interceptors behaviour

    agent.source.interceptors.foreach(interceptor => pw.write(agent.id + "." + Constants.sourceName + "." + agent.source.id + "." +
      interceptor + " = " + interceptor + "\n"))
  }

  def writeChannel(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + Constants.channelName.toUpperCase + " #####" + "\n\n")

    agent.channels.foreach(channel => pw.write(agent.id + "." + Constants.channelName + "." + channel.id + "." + Constants.typeName + " = " +
      channel._type + "\n"))

    agent.channels.foreach(channel => channel.settings.foreach(settings => pw.write(agent.id + "." + Constants.channelName + "." +
      channel.id + "." + settings.id + " = " + settings.value + "\n")))

  }

  def writeSink(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + Constants.sinkName.toUpperCase + " #####" + "\n\n")

    agent.sinks.foreach(sink => pw.write(agent.id + "." + Constants.sinkName + "." + sink.id + "." + Constants.typeName + " = " +
      sink._type + "\n"))

    agent.sinks.foreach(sink => sink.settings.foreach(settings => pw.write(agent.id + "." + Constants.sinkName + "." +
      sink.id + "." + settings.id + " = " + settings.value + "\n")))

  }

  def writeUnion(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + Constants.unionName.toUpperCase + " #####" + "\n\n")

    pw.write(agent.id + "." + Constants.sourceName + "." + agent.source.id + "." + Constants.channelName + " = " + agent.channels.map
      (chan => chan.id).reduce(_ + " " + _) + "\n")

    agent.sinks.foreach(sink => pw.write(agent.id + "." + Constants.sinkName + "." + sink.id + "." + "channel" + " = " + sink.channels.id + "\n"))

  }


}
