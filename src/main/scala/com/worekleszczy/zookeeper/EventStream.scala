package com.worekleszczy.zookeeper

import cats.MonadError
import cats.effect.kernel.{Async, Deferred}
import cats.syntax.functor._
import cats.syntax.monadError._
import com.worekleszczy.zookeeper.model.Path
import fs2._
import org.apache.zookeeper.WatchedEvent

object EventStream {

  def watchChildren[F[_]: Async: MonadError[*[_], Throwable]](
    zookeeper: Zookeeper[F],
    path: Path
  ): Stream[F, Vector[Path]] = {

    def completableWatcher(deferred: Deferred[F, WatchedEvent]): Watcher[F] = event => deferred.complete(event).void

    val childrenStream = for {
      signal <- Stream.eval(Deferred[F, WatchedEvent])
      watcher = completableWatcher(signal)
      children <- (Stream.eval(zookeeper.getChildren(path, watcher).rethrow) ++ Stream.eval(signal.get).drain)
    } yield children

    childrenStream.repeat

  }

}
