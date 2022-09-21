package com.worekleszczy.zookeeper

import cats.Functor
import cats.syntax.functor._
import org.apache.zookeeper.WatchedEvent

trait Watcher[F[_]] {
  def process(event: WatchedEvent): F[Unit]
}

object Watcher {

  def instance[F[_]: Functor](handle: WatchedEvent => F[_]): Watcher[F] = event => handle(event).void

}
