package com.stratio.ingestion.api.model.commons

/**
 * Created by eruiz on 16/10/15.
 */
abstract class Entity {
  val id: String
  val typo: String
  val name: String
  val description: String
}