package com.stratio.ingestion.api.utils

import com.google.gson.Gson
import com.stratio.ingestion.api.model.sink.Sink

/**
 * Created by eruiz on 15/10/15.
 */
object JsonParserToSink {
  val theGsonParser = new Gson()

  def parse(json: String) = {
    try {
      val sink = theGsonParser.fromJson(json, classOf[Sink])
      sink
    } catch {
      case _: Throwable =>
        throw new Exception("Unable to parse the Json")
    }

  }
}
