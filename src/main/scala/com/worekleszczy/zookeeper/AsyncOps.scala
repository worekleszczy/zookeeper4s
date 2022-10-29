package com.worekleszczy.zookeeper

import cats.data.EitherT
import cats.effect.{Async, Deferred, Resource}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import com.worekleszczy.zookeeper.Zookeeper.{Error, Result, ZookeeperClientError}
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.apache.zookeeper.data.Stat

trait AsyncOps[F[_]] extends Any {
  protected def zookeeper: Zookeeper[F]

  def unsafeGetDataBlock[T: ByteCodec](path: Path)(implicit async: Async[F]): F[T] = getDataBlock(path).rethrow

  //TODO it's an unsafe operation as it might remove other watchers too
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

  final def createEmptyIfNotExists(path: Path, mode: CreateMode)(implicit async: Async[F]): F[Result[(Path, Stat)]] =
    createIfNotExists(path, Array.empty, mode)

  final def createEncodeIfNotExists[T: ByteCodec](path: Path, obj: T, mode: CreateMode)(implicit
    async: Async[F]
  ): F[Result[(Path, Stat)]] =
    for {
      bytes  <- Async[F].fromTry(ByteCodec[T].encode(obj))
      result <- createIfNotExists(path, bytes, mode)
    } yield result

  def createIfNotExists(path: Path, bytes: Array[Byte], mode: CreateMode)(implicit
    async: Async[F]
  ): F[Result[(Path, Stat)]] =
    (for {
      exist <- EitherT(zookeeper.exists(path))
      result <- exist.fold(EitherT(zookeeper.create(path, bytes, mode)).recoverWith {
        case ZookeeperClientError(KeeperException.Code.NODEEXISTS) => EitherT(createIfNotExists(path, bytes, mode))
      })(stat => EitherT.rightT((path, stat)))
    } yield result).value

}
