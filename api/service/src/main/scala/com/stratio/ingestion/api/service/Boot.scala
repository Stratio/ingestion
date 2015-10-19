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
package com.stratio.ingestion.api.service

import akka.actor.{ ActorSystem, Props }
import akka.io.IO

import spray.can.Http

object Boot extends App {

	implicit val system = ActorSystem("my-actor-system")

	val api = system.actorOf(Props[ApiActor], "api-actor")

	IO(Http) ! Http.Bind(listener = api, interface = "0.0.0.0", port = 8080)
}