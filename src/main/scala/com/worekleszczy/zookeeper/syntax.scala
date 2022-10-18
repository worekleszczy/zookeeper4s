package com.worekleszczy.zookeeper

import cats.Functor
import cats.effect.Resource
import cats.syntax.either._
import cats.syntax.functor._
import com.worekleszczy.zookeeper.Zookeeper.{Result, ZookeeperClientError}
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.model.Path
import com.worekleszczy.zookeeper.watcher.WatcherHandler
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.data.Stat

object syntax {
  private def flattenOption[T, E[_]: Functor](
    effect: E[Result[Option[(T, Stat)]]]
  ): E[Result[(T, Stat)]] =
    effect.map(_.flatMap(_.fold(ZookeeperClientError(KeeperException.Code.NONODE).asLeft[(T, Stat)])(_.asRight)))

  final class UnsafeGetDataPartial[T, F[_]](private val zookeeper: Zookeeper[F]) extends AnyVal {
    def apply[E[_]](path: Path, watch: Watcher[F])(implicit
      handler: WatcherHandler.Aux[F, E],
      codec: ByteCodec[T],
      functor: Functor[E]
    ): E[Result[(T, Stat)]] = zookeeper.unsafeGetDataM[T, E](path, watch)
  }

  implicit final class ZookeeperOps[F[_]](protected val zookeeper: Zookeeper[F]) extends AnyVal with AsyncOps[F] {
    def unsafeGetDataM[T, E[_]](path: Path, watch: Watcher[F])(implicit
      handler: WatcherHandler.Aux[F, E],
      byteCodec: ByteCodec[T],
      functor: Functor[E]
    ): E[Result[(T, Stat)]] =
      flattenOption(
        zookeeper
          .getDataM[T, E](path, watch)
      )

    def unsafeGetData[T] = new UnsafeGetDataPartial[T, F](zookeeper)

    def unsafeGetData[T: ByteCodec](path: Path, watch: Boolean)(implicit functor: Functor[F]): F[Result[(T, Stat)]] =
      flattenOption(
        zookeeper
          .getData(path, watch)
      )

    final def getChildrenOnly[E[_]](path: Path, watcher: Watcher[F])(implicit
      watcherHandler: WatcherHandler.Aux[F, E],
      functor: Functor[E]
    ): E[Result[Vector[Path]]] = zookeeper.getChildren(path, watcher).map(_.map(_._1))

    final def getChildrenOnly(path: Path, watch: Boolean)(implicit functor: Functor[F]): F[Result[Vector[Path]]] =
      zookeeper.getChildren(path, watch).map(_.map(_._1))

    def existsR(path: Path, watcher: Watcher[F])(implicit
      handler: WatcherHandler.Aux[F, Resource[F, *]]
    ): Resource[F, Result[Option[Stat]]] = zookeeper.exists(path, watcher)

  }

}
