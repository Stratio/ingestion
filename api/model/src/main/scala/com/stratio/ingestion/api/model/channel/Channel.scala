package com.stratio.ingestion.api.model.channel

import com.stratio.ingestion.api.model.Attribute
import com.stratio.ingestion.api.model.source.Source

/**
 * Created by eruiz on 15/10/15.
 */
case class Channel(id: String, typo: String, name: String, description: String, settings: Seq[Attribute], sources:
Seq[Source])
