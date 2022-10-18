package com.worekleszczy.zookeeper

import cats.data.EitherT
import cats.effect.{Async, Deferred, Resource}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import com.worekleszczy.zookeeper.Zookeeper.{Error, Result}
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.data.Stat

trait AsyncOps[F[_]] extends Any {
  protected def zookeeper: Zookeeper[F]

  def unsafeGetDataBlock[T: ByteCodec](path: Path)(implicit async: Async[F]): F[T] = getDataBlock(path).rethrow

  def getDataBlock[T: ByteCodec](path: Path)(implicit async: Async[F]): F[Result[T]] = {

    def getDataLoop: F[Result[T]] =
      (for {
        unblockWatcher <- EitherT.right[Error](Deferred[F, Unit])
        watcher = Watcher.instance(_ => unblockWatcher.complete(()).void)
        (stats, removeWatcher) <- EitherT(zookeeper.exists[Resource[F, *]](path, watcher).allocated.flatMap {
          case (result, cancel) =>
            result.fold[F[Result[(Option[Stat], F[Unit])]]](
              error => cancel.as(error.asLeft),
              s => Async[F].pure((s, cancel).asRight)
            )
        })
        getData =
          stats
            .map { _ =>
              zookeeper
                .getData[T](path, watch = false)
                .flatMap { result =>
                  result.fold[F[Result[T]]](
                    error => removeWatcher.as(error.asLeft),
                    _.fold(removeWatcher >> getDataLoop) {
                      case (value, _) => removeWatcher.as(value.asRight)
                    }
                  )

                }
            }
            .getOrElse(unblockWatcher.get >> getDataLoop)
        data <- EitherT(getData)
      } yield data).value

    getDataLoop
  }

}
