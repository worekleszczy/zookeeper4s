package com.worekleszczy.zookeeper

import scala.util._
import scala.util.control.NoStackTrace

sealed trait Context

object Context {

  final class InvalidContextPassed extends RuntimeException with NoStackTrace {
    override lazy val getMessage: String = "Invalid context passed"
  }

  case object Empty extends Context

  def decode(obj: AnyRef): Try[Context] =
    obj match {
      case c: Context => Success(c)
      case _          => Failure(new InvalidContextPassed)
    }
}
