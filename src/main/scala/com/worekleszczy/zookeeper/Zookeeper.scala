package com.worekleszczy.zookeeper

import cats.effect._
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.syntax.functor._
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.{WatchedEvent, Watcher => AWatcher, ZooKeeper => AZooKeeper}
import com.softwaremill.tagging._
import org.apache.zookeeper.data.Stat
import cats.syntax.option._
import org.apache.zookeeper.AsyncCallback.Children2Callback
import scala.jdk.CollectionConverters._

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

trait Watcher[F[_]] {
  def process(event: WatchedEvent): F[Unit]
}

sealed trait ResultCode

sealed trait Context

object Context {

  case object Empty extends Context
}

trait Zookeeper[F[_]] {

  def watch(path: Path): F[Array[Byte]]

  def getChildren(path: Path, watch: Option[WatchType[F]]): F[Vector[Path]]

  def getChildren(path: Path, watcherFs: Watcher[F]): F[Vector[Path]] = {

    getChildren(path, WatchType.SingleWatcher[F](watcherFs).some)
  }

  def getChildren(path: Path, watch: Boolean): F[Vector[Path]] =
    getChildren(path, Option.when(watch)(WatchType.DefaultWatcher))

  def getChildrenWithInfo(
    path: Path,
    watch: Option[WatchType[F]]
  ): F[(Int @@ ResultCode, Path, Context, Vector[Path], Stat)]
  def getChildrenWithInfo(
    path: Path,
    watcherFs: Watcher[F]
  ): F[(Int @@ ResultCode, Path, Context, Vector[Path], Stat)] =
    getChildrenWithInfo(path, WatchType.SingleWatcher(watcherFs).some)

  def getChildrenWithInfo(path: Path, watch: Boolean): F[(Int @@ ResultCode, Path, Context, Vector[Path], Stat)] =
    getChildrenWithInfo(path, Option.when(watch)(WatchType.DefaultWatcher))

}

object Zookeeper {

  val noopWatcher: AWatcher = _ => ()

  def apply[F[_]: Async](
    config: ZookeeperConfig,
    dispatcher: Dispatcher[F],
    watcher: AWatcher
  ): Resource[F, Zookeeper[F]] =
    Resource
      .make[F, ZookeeperLive[F]] {
        Sync[F].delay {
          new ZookeeperLive(new AZooKeeper(config.host, config.timeout.toMillis.toInt, watcher), dispatcher, config)
        }
      }(_.close)
      .widen[Zookeeper[F]]

  def apply[F[_]: Async](
    config: ZookeeperConfig,
    watcher: AWatcher = noopWatcher
  ): Resource[F, Zookeeper[F]] = Dispatcher[F].flatMap(apply(config, _, watcher))

  private final class ZookeeperLive[F[_]: Async](
    underlying: AZooKeeper,
    dispatcher: Dispatcher[F],
    config: ZookeeperConfig
  ) extends Zookeeper[F] {

    def watch(path: Path): F[Array[Byte]] = ???

    private[zookeeper] def close: F[Unit] = Sync[F].delay(underlying.close())

    def getChildren(path: Path, watch: Option[WatchType[F]]): F[Vector[Path]] =
      getChildrenWithInfo(path, watch).map(_._4)

    def getChildrenWithInfo(
      path: Path,
      watch: Option[WatchType[F]]
    ): F[(Int @@ ResultCode, Path, Context, Vector[Path], Stat)] = {

      val rebasedPath = path.rebase(config.root)

      Async[F].async_[(Int @@ ResultCode, Path, Context, Vector[Path], Stat)] { callback =>
        val cb: Children2Callback = (rc, rawPath, context, childrenRaw, stat) => {

          context match {
            case c: Context =>
              val path: Path             = Path.unsafeFromString(rawPath)
              val children: Vector[Path] = childrenRaw.asScala.to(Vector).map(p => Path.unsafeFromString(s"/$p"))

              callback(Right((rc.taggedWith[ResultCode], path, c, children, stat)))

            case _ => callback(Left(new RuntimeException("Invalid context received!")))
          }
        }

        watch match {
          case Some(WatchType.DefaultWatcher) => underlying.getChildren(rebasedPath.raw, true, cb, Context.Empty)
          case Some(WatchType.SingleWatcher(watcher)) =>
            val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))

            underlying.getChildren(rebasedPath.raw, unsafeWatcher, cb, Context.Empty)

          case None => underlying.getChildren(rebasedPath.raw, false, cb, Context.Empty)
        }
      }

    }

  }
}
