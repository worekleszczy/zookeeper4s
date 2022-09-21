package com.worekleszczy.zookeeper

sealed trait WatchType[+F[_]]

object WatchType {
  case object DefaultWatcher extends WatchType[Nothing]

  /**
    * This watcher will be called only once
    * @param watcher
    * @tparam F
    */
  case class SingleWatcher[F[_]](watcher: Watcher[F]) extends WatchType[F]
}
