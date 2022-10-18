package com.worekleszczy.zookeeper.watcher

import cats.MonadError
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.syntax.either._
import cats.syntax.monadError._
import com.worekleszczy.zookeeper.Zookeeper.{Error, Result}
import com.worekleszczy.zookeeper.model.Path
import com.worekleszczy.zookeeper.{Context, Zookeeper}
import org.apache.zookeeper.AsyncCallback.VoidCallback
import org.apache.zookeeper.Watcher.WatcherType
import org.apache.zookeeper.{Watcher => AWatcher, ZooKeeper => AZookeeper}

import scala.annotation.unused

trait WatcherHandler[F[_]] {
  type E[_]
  def registerCleanUp[T](
    value: F[T],
    path: Path,
    watcherType: WatcherType,
    zookeeper: AZookeeper,
    unsafeWatcher: AWatcher
  ): E[T]
}

object WatcherHandler extends NonPriorityWatcherHandler {

  type Aux[F[_], EE[_]] = WatcherHandler[F] { type E[x] = EE[x] }

  implicit def identityHandler[F[_]]: WatcherHandler.Aux[F, F] =
    new WatcherHandler[F] {
      type E[x] = F[x]

      def registerCleanUp[T](
        value: F[T],
        @unused path: Path,
        @unused watcherType: WatcherType,
        @unused zookeeper: AZookeeper,
        @unused unsafeWatcher: AWatcher
      ): E[T] = value
    }

}

sealed trait NonPriorityWatcherHandler {
  implicit def resourceIoHandler[F[_]: Async: MonadError[*[_], Throwable]]: WatcherHandler.Aux[F, Resource[F, *]] = {
    new WatcherHandler[F] {
      type E[x] = Resource[F, x]

      def registerCleanUp[T](
        value: F[T],
        path: Path,
        watcherType: WatcherType,
        zookeeper: AZookeeper,
        unsafeWatcher: AWatcher
      ): E[T] =
        Resource.make(value) { _ =>
          Async[F]
            .async_[Result[Unit]] { callback =>
              val cb: VoidCallback = (rc, _, context) => {
                val result = Context
                  .decode(context)
                  .map { _ =>
                    Zookeeper.onSuccess(rc)(().asRight[Error])
                  }
                  .toEither

                callback(result)
              }
              zookeeper.removeWatches(path.raw, unsafeWatcher, watcherType, false, cb, Context.Empty)
            }
            .rethrow

        }
    }
  }
}
