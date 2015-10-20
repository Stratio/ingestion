package com.stratio.ingestion.api.service.utils

import shapeless._
import spray.routing.PathMatcher

import scala.util.Try

trait VersionDirectives {

  val Version = PathMatcher( """v([0-9]+)""".r)
    .hflatMap {
    case vString :: HNil =>
      Try(Integer.parseInt(vString) :: HNil).toOption
  }
}