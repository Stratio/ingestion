package com.stratio.ingestion.api.utils

import java.io.{File, PrintWriter}

import com.stratio.ingestion.api.model.commons.Agent

/**
 * Created by eruiz on 16/10/15.
 */

object ModelToProperties {
  //TODO Sacar estas variables a un fichero de configuracion.
  val agentName = "a1"
  val sourceName = "sources"
  val interceptorName = "interceptors"
  val channelName = "channels"
  val sinkName = "sinks"
  val unionName = "union"

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

    pw.write(agentName + "." + sourceName + " = " + agent.source.name + "\n")
    pw.write(agentName + "." + channelName + " = " + agent.channels.map(chan => chan.name).reduce(_ + " " + _) + "\n")
    pw.write(agentName + "." + sinkName + " = " + agent.sinks.map(sink => sink.name).reduce(_ + " " + _) + "\n")
  }

  def writeSource(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + sourceName.toUpperCase + " #####" + "\n\n")
    pw.write(agentName + "." + sourceName + "." + agent.source.name + "." + "type = " + agent.source.typo + "\n")
    agent.source.settings.map(settings => pw.write(agentName + "." + sourceName + "." + agent.source.name + "." +
      settings.name + " = " + settings.value + "\n"))
    pw.write("\n\n##### " + interceptorName.toUpperCase + " #####" + "\n\n")
    //TODO Ver la manera en que se define un interceptor, si es como un setting más o si hay que definirse una case class de interceptores

    agent.source.interceptors.map(interceptor => pw.write(agentName + "." + sourceName + "." + agent.source.name + "." +
      interceptor + " = " + interceptor + "\n"))
  }

  def writeChannel(agent: Agent, pw: PrintWriter): Unit = {
    pw.write("\n\n##### " + channelName.toUpperCase() + " #####" + "\n\n")

    agent.channels.map(channel => pw.write(agentName + "." + channelName + "." + channel.name + "." + "type = " +
      channel.typo + "\n"))

    agent.channels.map(channel => channel.settings.map(settings => pw.write(agentName + "." + channelName + "." +
      channel.name + "." + settings.name + " = " + settings.value + "\n")))

  }

  def writeSink(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + sinkName.toUpperCase + " #####" + "\n\n")

    agent.sinks.map(sink => pw.write(agentName + "." + sinkName + "." + sink.name + "." + "type = " +
      sink.typo + "\n"))

    agent.sinks.map(sink => sink.settings.map(settings => pw.write(agentName + "." + sinkName + "." +
      sink.name + "." + settings.name + " = " + settings.value + "\n")))

  }

  def writeUnion(agent: Agent, pw: PrintWriter): Unit = {

    pw.write("\n\n##### " + unionName.toUpperCase + " #####" + "\n\n")

    pw.write(agentName + "." + sourceName + "." + agent.source.name +"." + channelName + " = " + agent.channels.map
    (chan => chan
      .name)
      .reduce
      (_ +
      " " + _) +
      "\n")


//    agent.channels.map(channel => channel.sources.map(source => pw.write(agentName + "." + sourceName + "." +
//      source.name + "." + channelName + " = " + channel.name + "\n")))

    agent.sinks.map(sink => sink.channels.map(channels => pw.write(agentName + "." + sinkName + "." +
      sink.name + "." + "channel" + " = " + channels.name + "\n")))

  }


}
