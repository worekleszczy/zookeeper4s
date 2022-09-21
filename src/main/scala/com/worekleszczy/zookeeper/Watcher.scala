package com.worekleszczy.zookeeper

import org.apache.zookeeper.WatchedEvent

trait Watcher[F[_]] {
  def process(event: WatchedEvent): F[Unit]
}
