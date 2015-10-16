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

import com.stratio.ingestion.api.model.channel.Channel
import com.stratio.ingestion.api.model.sink.Sink
import JsonParser._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import spray.json._

/**
 * Created by eruiz on 15/10/15.
 */

@RunWith(classOf[JUnitRunner])
class JsonParserToSinkTest extends FunSpec
with GivenWhenThen
with ShouldMatchers {

  describe("The Json Parser") {
    it("should parse this sink from the json") {
      Given("a json sink")

      val sink = Sink("1", "typo", "mongo", "mongoSink", Seq(), Seq())
      sink should be(sink.toJson.convertTo[Sink])

      val channel = Channel("1", "typo", "mongo", "mongoSink", Seq(), Seq())
      val sink2 = Sink("1", "typo", "mongo", "mongoSink", Seq(), Seq(channel))
      sink2 should be(sink2.toJson.convertTo[Sink])
    }


  }
}
