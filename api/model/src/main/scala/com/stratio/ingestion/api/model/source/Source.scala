package com.stratio.ingestion.api.model.source

import java.nio.channels.Channels

import com.stratio.ingestion.api.model.Attribute



/**
 * Created by eruiz on 15/10/15.
 */
case class Source(id: String, typo: String, name: String, description: String, interceptors: Seq[String], settings:
Seq[Attribute], channels:Seq[Channels])
