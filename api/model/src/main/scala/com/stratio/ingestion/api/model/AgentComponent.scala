package com.stratio.ingestion.api.model

import com.stratio.ingestion.api.model.channel.Channel
import com.stratio.ingestion.api.model.sink.Sink
import com.stratio.ingestion.api.model.source.Source

/**
 * Created by eruiz on 15/10/15.
 */
case class AgentComponent(sources: Seq[Source], channels: Seq[Channel], sinks: Seq[Sink]) {


  def formatString(inputString: String) = {
    inputString.replace("\"", "")
    inputString.replace("[", "")
    inputString.replace("]", "")
    inputString
  }
}