package com.worekleszczy.zookeeper

import cats.MonadError
import cats.data.EitherT
import cats.effect._
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.option._
import com.worekleszczy.zookeeper.Zookeeper.Result
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.Path
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{CreateMode, KeeperException, Watcher => AWatcher, ZooKeeper => AZooKeeper}
import org.typelevel.log4cats.Logger

import scala.jdk.CollectionConverters._
import scala.util._
import scala.util.control.NoStackTrace

trait Zookeeper[F[_]] {

  def getChildren(path: Path, watch: Option[WatchType[F]]): F[Result[Vector[Path]]]

  final def getChildren(path: Path, watcherFs: Watcher[F]): F[Result[Vector[Path]]] =
    getChildren(path, WatchType.SingleWatcher[F](watcherFs).some)

  final def getChildren(path: Path, watch: Boolean): F[Result[Vector[Path]]] =
    getChildren(path, Option.when(watch)(WatchType.DefaultWatcher))

  def getChildrenWithStat(
    path: Path,
    watch: Option[WatchType[F]]
  ): F[Result[(Vector[Path], Stat)]]

  final def getChildrenWithStat(
    path: Path,
    watcherFs: Watcher[F]
  ): F[Result[(Vector[Path], Stat)]] =
    getChildrenWithStat(path, WatchType.SingleWatcher(watcherFs).some)

  final def getChildrenWithStat(path: Path, watch: Boolean): F[Result[(Vector[Path], Stat)]] =
    getChildrenWithStat(path, Option.when(watch)(WatchType.DefaultWatcher))

  def getData[T: ByteCodec](path: Path, watch: Option[WatchType[F]]): F[Result[(T, Stat)]]

  final def getData[T: ByteCodec](path: Path, watch: Boolean): F[Result[(T, Stat)]] =
    getData(path, Option.when(watch)(WatchType.DefaultWatcher))

  final def getData[T: ByteCodec](path: Path, watch: Watcher[F]): F[Result[(T, Stat)]] =
    getData(path, WatchType.SingleWatcher(watch).some)

  def create(path: Path, data: Array[Byte], mode: CreateMode): F[Result[(Path, Stat)]]

  final def createEmpty(path: Path, mode: CreateMode): F[Result[(Path, Stat)]] = create(path, Array.empty[Byte], mode)

  def createEncode[T: ByteCodec](path: Path, obj: T, mode: CreateMode): F[Result[(Path, Stat)]]

  def delete(path: Path, version: Int): F[Result[Unit]]

  def setData(path: Path, body: Array[Byte], version: Int): F[Result[Stat]]

  def setDataEncode[T: ByteCodec](path: Path, obj: T, version: Int): F[Result[Stat]]

  def exists(path: Path, watch: Option[WatchType[F]]): F[Result[Option[Stat]]]

  def exists(path: Path, watch: Boolean): F[Result[Option[Stat]]] =
    exists(path, Option.when(watch)(WatchType.DefaultWatcher))

  def exists(path: Path, watch: Watcher[F]): F[Result[Option[Stat]]] = exists(path, WatchType.SingleWatcher(watch).some)
}

object Zookeeper {

  type Result[+T] = Either[Error, T]

  sealed trait Error extends RuntimeException with NoStackTrace

  case class ZookeeperClientError(code: KeeperException.Code) extends Error

  case object DecodeError extends Error

  val noopWatcher: AWatcher = _ => ()

  def apply[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig,
    dispatcher: Dispatcher[F],
    watcher: AWatcher
  ): Resource[F, Zookeeper[F]] =
    createLiveZookeeper(config, dispatcher, watcher).widen[Zookeeper[F]]

  def apply[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig,
    watcher: AWatcher = noopWatcher
  ): Resource[F, Zookeeper[F]] = Dispatcher[F].flatMap(apply(config, _, watcher))

  private[zookeeper] def createLiveZookeeper[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig,
    dispatcher: Dispatcher[F],
    watcher: AWatcher
  ): Resource[F, ZookeeperLive[F]] =
    Resource
      .make[F, ZookeeperLive[F]] {
        val zookeeper = Sync[F].delay {
          new ZookeeperLive(
            new AZooKeeper(s"${config.host}:${config.port}", config.timeout.toMillis.toInt, watcher),
            dispatcher,
            config
          )
        }

        if (!config.createRootIfNotExists || (config.createRootIfNotExists && config.root.level == 0)) {
          zookeeper
        } else if (config.createRootIfNotExists && config.root.level == 1) {

          for {
            z     <- zookeeper
            stats <- z.existsImpl(config.root, none, relative = false).rethrow
            _ <-
              stats
                .as(Async[F].unit)
                .getOrElse(z.createImpl(config.root, Array.empty, CreateMode.PERSISTENT, relative = false).rethrow.void)
          } yield z
        } else Sync[F].raiseError(new RuntimeException("Creating root nodes above level 1 is not supported"))
      }(_.close)

  private[zookeeper] final class ZookeeperLive[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    underlying: AZooKeeper,
    dispatcher: Dispatcher[F],
    config: ZookeeperConfig
  ) extends Zookeeper[F] {

    private[zookeeper] def close: F[Unit] = {

//      if (config.removeRootOnExit) {
//        getChildrenWithStatImpl(Path.root, none, false).map
//      }
      Sync[F].delay(underlying.close())
    }

    def getChildren(path: Path, watch: Option[WatchType[F]]): F[Result[Vector[Path]]] =
      EitherT(getChildrenWithStat(path, watch)).map(_._1).value

    private[zookeeper] def getChildrenWithStatImpl(
      path: Path,
      watch: Option[WatchType[F]],
      relative: Boolean
    ): F[Result[(Vector[Path], Stat)]] = {

      val transformed = if (relative) rebaseOnRoot(path) else path

      Async[F].async_[Result[(Vector[Path], Stat)]] { callback =>
        val cb: Children2Callback = (rc, _, context, childrenRaw, stat) => {

          callback(
            Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {
                  val childrenTransformation: String => Path =
                    if (relative) (Path.unsafeAsAbsolutePath _) andThen (_.stripBase(config.root))
                    else Path.unsafeAsAbsolutePath

                  val children: Vector[Path] =
                    childrenRaw.asScala
                      .to(Vector)
                      .map(childrenTransformation)

                  (children, stat).asRight
                }

              }
              .toEither
          )
        }

        watch match {
          case Some(WatchType.SingleWatcher(watcher)) =>
            val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))

            underlying.getChildren(transformed.raw, unsafeWatcher, cb, Context.Empty)

          case Some(WatchType.DefaultWatcher) => underlying.getChildren(transformed.raw, true, cb, Context.Empty)
          case None                           => underlying.getChildren(transformed.raw, false, cb, Context.Empty)
        }
      }

    }

    def getChildrenWithStat(
      path: Path,
      watch: Option[WatchType[F]]
    ): F[Result[(Vector[Path], Stat)]] = getChildrenWithStatImpl(path, watch, relative = true)

    def getData[T: ByteCodec](path: Path, watch: Option[WatchType[F]]): F[Either[Error, (T, Stat)]] = {

      val transformedPath = rebaseOnRoot(path)
      Async[F]
        .async_[(Either[Error, (T, Stat)])] { callback =>
          val cb: DataCallback = (_, _, context, data, stat) => {
            val result = Context
              .decode(context)
              .map { _ =>
                ByteCodec[T].decode(data).toEither.bimap(_ => DecodeError, _ -> stat)

              }
              .toEither

            callback(result)
          }

          watch match {
            case Some(WatchType.SingleWatcher(watcher)) =>
              val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))

              underlying.getData(transformedPath.raw, unsafeWatcher, cb, Context.Empty)
            case Some(WatchType.DefaultWatcher) => underlying.getData(transformedPath.raw, true, cb, Context.Empty)
            case None                           => underlying.getData(transformedPath.raw, false, cb, Context.Empty)
          }
        }
    }

    private[zookeeper] def createImpl(
      path: Path,
      data: Array[Byte],
      mode: CreateMode,
      relative: Boolean
    ): F[Result[(Path, Stat)]] = {
      val transformedPath = if (relative) rebaseOnRoot(path) else path
      Async[F]
        .async_[Result[(Path, Stat)]] { callback =>
          val cb: Create2Callback = (rc, submittedPath, context, name, stat) => {
            val result = Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {
                  val path = Path.unsafeFromPathAndName(submittedPath, name)
                  (if (relative) path.stripBase(config.root) else path, stat).asRight[Error]
                }
              }
              .toEither

            callback(result)
          }
          underlying.create(transformedPath.raw, data, Ids.OPEN_ACL_UNSAFE, mode, cb, Context.Empty)
        }
    }

    def create(path: Path, data: Array[Byte], mode: CreateMode): F[Result[(Path, Stat)]] =
      createImpl(path, data, mode, relative = true)

    def createEncode[T: ByteCodec](path: Path, obj: T, mode: CreateMode): F[Result[(Path, Stat)]] =
      for {
        encoded <- Sync[F].fromTry(ByteCodec[T].encode(obj))
        result  <- create(path, encoded, mode)
      } yield result

    def delete(path: Path, version: Int): F[Result[Unit]] = {
      val transformedPath = rebaseOnRoot(path)
      Async[F]
        .async_[Result[Unit]] { callback =>
          val cb: VoidCallback = (rc, _, context) => {
            val result = Context
              .decode(context)
              .map { _ =>
                onSuccess(rc)(().asRight[Error])
              }
              .toEither

            callback(result)
          }

          underlying.delete(transformedPath.raw, version, cb, Context.Empty)
        }

    }

    def setData(path: Path, body: Array[Byte], version: Int): F[Result[Stat]] = {
      val transformedPath = rebaseOnRoot(path)
      Async[F]
        .async_[Result[Stat]] { callback =>
          val cb: StatCallback = (rc, _, context, stat) => {
            val result = Context
              .decode(context)
              .map { _ =>
                onSuccess(rc)(stat.asRight[Error])
              }
              .toEither

            callback(result)
          }

          underlying.setData(transformedPath.raw, body, version, cb, Context.Empty)
        }
    }

    def exists(path: Path, watch: Option[WatchType[F]]): F[Result[Option[Stat]]] =
      existsImpl(path, watch, relative = true)

    private[zookeeper] def existsImpl(
      path: Path,
      watch: Option[WatchType[F]],
      relative: Boolean
    ): F[Result[Option[Stat]]] = {

      val transformed = if (relative) rebaseOnRoot(path) else path

      Async[F].async_[Result[Option[Stat]]] { callback =>
        val cb: StatCallback = (rc, passed, context, stat) => {

          callback(
            Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {
                  stat.some.asRight
                }.recover {
                  case ZookeeperClientError(KeeperException.Code.NONODE) => none
                }
              }
              .toEither
          )
        }

        watch match {
          case Some(WatchType.SingleWatcher(watcher)) =>
            val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))

            underlying.exists(transformed.raw, unsafeWatcher, cb, Context.Empty)

          case Some(WatchType.DefaultWatcher) => underlying.exists(transformed.raw, true, cb, Context.Empty)

          case None => underlying.exists(transformed.raw, false, cb, Context.Empty)
        }
      }
    }

    def setDataEncode[T: ByteCodec](path: Path, obj: T, version: Int): F[Result[Stat]] =
      for {
        body <- Sync[F].fromTry(ByteCodec[T].encode(obj))
        stat <- setData(path, body, version)
      } yield stat

    private def rebaseOnRoot(path: Path): Path = path.rebase(config.root)

    private def onSuccess[T](rc: Int)(success: => Either[Error, T]): Either[Error, T] =
      KeeperException.Code.get(rc) match {
        case KeeperException.Code.OK =>
          success
        case other =>
          ZookeeperClientError(other).asLeft[T].leftWiden[Error]
      }
  }
}
