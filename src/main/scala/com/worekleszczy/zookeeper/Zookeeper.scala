package com.worekleszczy.zookeeper

import cats.data.EitherT
import cats.effect._
import cats.effect.kernel.Resource
import cats.effect.std.Dispatcher
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.option._
import cats.{Applicative, MonadError}
import com.worekleszczy.zookeeper.Zookeeper.Result
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.{Path, SequentialContext}
import org.apache.zookeeper.AsyncCallback._
import org.apache.zookeeper.Watcher.WatcherType
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.{
  AddWatchMode,
  CreateMode,
  KeeperException,
  ZKUtil,
  Watcher => AWatcher,
  ZooKeeper => AZooKeeper
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.syntax._

import scala.jdk.CollectionConverters._
import scala.util._
import scala.util.chaining._
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

  def deleteRecursive(path: Path): F[Result[Unit]]

  def setData(path: Path, body: Array[Byte], version: Int): F[Result[Stat]]

  def setDataEncode[T: ByteCodec](path: Path, obj: T, version: Int): F[Result[Stat]]

  def exists(path: Path, watch: Option[WatchType[F]]): F[Result[Option[Stat]]]

  def exists(path: Path, watch: Boolean): F[Result[Option[Stat]]] =
    exists(path, Option.when(watch)(WatchType.DefaultWatcher))

  def exists(path: Path, watch: Watcher[F]): F[Result[Option[Stat]]] = exists(path, WatchType.SingleWatcher(watch).some)

  def addWatcher(path: Path, watcher: Watcher[F], mode: AddWatchMode): Resource[F, Unit]
}

object Zookeeper {

  type Result[+T] = Either[Error, T]

  sealed trait Error extends RuntimeException with NoStackTrace

  case class ZookeeperClientError(code: KeeperException.Code) extends Error {
    override def getMessage: String = s"Error $code occurred"
  }

  case object DecodeError extends Error

  def noopWatcher[F[_]: Applicative]: Watcher[F] = Watcher.instance(_ => Applicative[F].pure(()))

  def apply[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig,
    dispatcher: Dispatcher[F],
    watcher: Watcher[F]
  ): Resource[F, Zookeeper[F]] =
    createLiveZookeeper(config, dispatcher, watcher).widen[Zookeeper[F]]

  def apply[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig
  ): Resource[F, Zookeeper[F]] = Dispatcher[F].flatMap(apply(config, _, noopWatcher[F]))

  private[zookeeper] def createLiveZookeeper[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    config: ZookeeperConfig,
    dispatcher: Dispatcher[F],
    watcher: Watcher[F]
  ): Resource[F, ZookeeperLive[F]] =
    Resource
      .make[F, ZookeeperLive[F]] {

        val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))
        val zookeeper = Sync[F].delay {
          new ZookeeperLive(
            new AZooKeeper(s"${config.host}:${config.port}", config.timeout.toMillis.toInt, unsafeWatcher),
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

    private final val serialSeparator = ':'

    private[zookeeper] def close: F[Unit] = {

      val removeRoot = if (config.removeRootOnExit) {
        for {
          rootStats <- exists(Path.root, watch = false).rethrow
          _         <- info"Deleting root path ${config.root.raw}"
          _         <- rootStats.map(_ => deleteRecursive(Path.root).rethrow).sequence_
        } yield ()
      } else Async[F].unit

      removeRoot >> Sync[F].delay(underlying.close())
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
        val cb: Children2Callback = (rc, baseRaw, context, childrenRaw, stat) => {

          callback(
            Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {

                  val base = Path.unsafeFromString(baseRaw)

                  val childrenTransformation: Path => Path =
                    if (relative) (readPathName _) andThen (_.stripBase(config.root))
                    else readPathName

                  (
                    childrenRaw.asScala
                      .to(Vector)
                      .map(child => childrenTransformation(base.resolve(child))),
                    stat
                  ).asRight

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

      val absolutePath = rebaseOnRoot(path)
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

              underlying.getData(absolutePath.raw, unsafeWatcher, cb, Context.Empty)
            case Some(WatchType.DefaultWatcher) => underlying.getData(absolutePath.raw, true, cb, Context.Empty)
            case None                           => underlying.getData(absolutePath.raw, false, cb, Context.Empty)
          }
        }
    }

    private[zookeeper] def createImpl(
      path: Path,
      data: Array[Byte],
      mode: CreateMode,
      relative: Boolean
    ): F[Result[(Path, Stat)]] = {
      val absolutePath = (if (relative) rebaseOnRoot(path) else path)
        .pipe { abs =>
          mode match {
            case CreateMode.EPHEMERAL_SEQUENTIAL | CreateMode.PERSISTENT_SEQUENTIAL |
                CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL =>
              abs.transformFileName(_ + serialSeparator)
            case _ => abs
          }

        }

      Async[F]
        .async_[Result[(Path, Stat)]] { callback =>
          val cb: Create2Callback = (rc, _, context, name, stat) => {
            val result = Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {
                  readPathName(Path.unsafeFromString(name)).pipe { path =>
                    (if (relative) path.stripBase(config.root) else path, stat).asRight[Error]
                  }
                }
              }
              .toEither

            callback(result)
          }
          underlying.create(absolutePath.raw, data, Ids.OPEN_ACL_UNSAFE, mode, cb, Context.Empty)
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
      val absolutePath = rebaseOnRoot(path)
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
          underlying.delete(absolutePath.raw, version, cb, Context.Empty)
        }

    }

    def deleteRecursive(path: Path): F[Result[Unit]] = {
      val absolutePath = rebaseOnRoot(path)
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

          ZKUtil.deleteRecursive(underlying, absolutePath.raw, cb, Context.Empty)
        }

    }

    def setData(path: Path, body: Array[Byte], version: Int): F[Result[Stat]] = {
      val absolutePath = rebaseOnRoot(path)
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

          underlying.setData(absolutePath.raw, body, version, cb, Context.Empty)
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
        val cb: StatCallback = (rc, _, context, stat) => {

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

    def addWatcher(path: Path, watcher: Watcher[F], mode: AddWatchMode): Resource[F, Unit] = {

      val transformed             = rebaseOnRoot(path)
      val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watcher.process(event))

      Resource.make[F, Unit] {
        Sync[F].delay {
          underlying.addWatch(transformed.raw, unsafeWatcher, mode)
        }
      } { _ =>
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
            underlying.removeWatches(transformed.raw, unsafeWatcher, WatcherType.Any, false, cb, Context.Empty)
          }
          .rethrow
      }
    }

    def setDataEncode[T: ByteCodec](path: Path, obj: T, version: Int): F[Result[Stat]] =
      for {
        body <- Sync[F].fromTry(ByteCodec[T].encode(obj))
        stat <- setData(path, body, version)
      } yield stat

    private def rebaseOnRoot(path: Path): Path = path.rebase(config.root)

    private def readPathName(path: Path): Path =
      path.extractSequential {
        _.split(serialSeparator) match {
          case Array(name, serial) => serial.toLongOption.map(value => SequentialContext(name, value))
          case _                   => None

        }

      }

    private def onSuccess[T](rc: Int)(success: => Either[Error, T]): Either[Error, T] = {
      KeeperException.Code.get(rc) match {
        case KeeperException.Code.OK =>
          success
        case other =>
          ZookeeperClientError(other).asLeft[T].leftWiden[Error]
      }
    }
  }
}
