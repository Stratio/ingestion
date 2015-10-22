package com.stratio.ingestion.api.model.validations

import com.stratio.ingestion.api.model.commons.Attribute

import scala.collection.mutable.ListBuffer

/**
 * Created by miguelsegura on 20/10/15.
 */
class ModelValidations {

  private var listMessages2 : List[String] = List();
  private val listMessages = ListBuffer.empty[String]

  def checkSourceChannel(channel: String) : Unit = {
    if(channel.isEmpty){
        listMessages += "A source isn't connected to a channel"
    }
  }

  def checkSinkChannel(channel: String) : Unit = {
    if(channel.isEmpty){
      listMessages += "A sink isn't connected to a channel"
    }
  }

  def checkChannelSource(source: String) : Unit = {
    if(source.isEmpty){
      listMessages += "A channel isn't connected to a source"
    }
  }

  def checkRequiredValue(attribute: Attribute) : Unit = {
//    if(attribute.value.is){d
//      listMessages += "A source isn't connected to a channel"
//    }
  }
}
