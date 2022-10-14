package com.worekleszczy.zookeeper

import cats.data.EitherT
import cats.effect._
import cats.effect.std.Dispatcher
import cats.syntax.apply._
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.monadError._
import cats.syntax.option._
import cats.{Applicative, Functor, MonadError}
import com.worekleszczy.zookeeper.Zookeeper.{Result, ZookeeperClientError}
import com.worekleszczy.zookeeper.codec.ByteCodec
import com.worekleszczy.zookeeper.config.ZookeeperConfig
import com.worekleszczy.zookeeper.model.{Path, SequentialContext}
import com.worekleszczy.zookeeper.watcher.WatcherHandler
import com.worekleszczy.zookeeper.watcher.WatcherHandler.Aux
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

  def getData[T: ByteCodec](path: Path, watch: Option[WatchType[F]]): F[Result[Option[(T, Stat)]]]

  final def getData[T: ByteCodec](path: Path, watch: Boolean): F[Result[Option[(T, Stat)]]] =
    getData(path, Option.when(watch)(WatchType.DefaultWatcher))

  final def getData[T: ByteCodec](path: Path, watch: Watcher[F]): F[Result[Option[(T, Stat)]]] =
    getData(path, WatchType.SingleWatcher(watch).some)

  def create(path: Path, data: Array[Byte], mode: CreateMode): F[Result[(Path, Stat)]]

  final def createEmpty(path: Path, mode: CreateMode): F[Result[(Path, Stat)]] = create(path, Array.empty[Byte], mode)

  def createEncode[T: ByteCodec](path: Path, obj: T, mode: CreateMode): F[Result[(Path, Stat)]]

  def delete(path: Path, version: Int): F[Result[Unit]]

  def deleteRecursive(path: Path): F[Result[Unit]]

  def setData(path: Path, body: Array[Byte], version: Int): F[Result[Stat]]

  def setDataEncode[T: ByteCodec](path: Path, obj: T, version: Int): F[Result[Stat]]

  def exists(path: Path, watch: Boolean = false): F[Result[Option[Stat]]]

  def exists[E[_]](path: Path, watch: Watcher[F])(implicit handler: WatcherHandler.Aux[F, E]): E[Result[Option[Stat]]]

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
            config,
            relative = true
          )
        }

        if (!config.createRootIfNotExists || (config.createRootIfNotExists && config.root.level == 0)) {
          zookeeper
        } else if (config.createRootIfNotExists && config.root.level == 1) {

          (for {
            z     <- zookeeper.map(_.asAbsolute)
            stats <- z.exists(config.root).rethrow
            _ <-
              stats
                .as(Async[F].unit)
                .getOrElse(z.create(config.root, Array.empty, CreateMode.PERSISTENT).rethrow.void)
          } yield ()) *> zookeeper
        } else Sync[F].raiseError(new RuntimeException("Creating root nodes above level 1 is not supported"))
      }(_.close)

  def onSuccess[T](rc: Int)(success: => Either[Error, T]): Either[Error, T] = {
    KeeperException.Code.get(rc) match {
      case KeeperException.Code.OK =>
        success
      case other =>
        ZookeeperClientError(other).asLeft[T].leftWiden[Error]
    }
  }

  private[zookeeper] final class ZookeeperLive[F[_]: Async: Logger: MonadError[*[_], Throwable]](
    underlying: AZooKeeper,
    dispatcher: Dispatcher[F],
    config: ZookeeperConfig,
    relative: Boolean
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
      watch: Option[WatchType[F]]
    ): F[Result[(Vector[Path], Stat)]] = {

      val transformed = transformRelative(path)

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
    ): F[Result[(Vector[Path], Stat)]] = getChildrenWithStatImpl(path, watch)

    def getData[T: ByteCodec](path: Path, watch: Option[WatchType[F]]): F[Either[Error, Option[(T, Stat)]]] = {

      val absolutePath = transformRelative(path)
      Async[F]
        .async_[(Either[Error, Option[(T, Stat)]])] { callback =>
          val cb: DataCallback = (rc, _, context, data, stat) => {

            val result = Context
              .decode(context)
              .map { _ =>
                onSuccess(rc) {
                  ByteCodec[T].decode(data).toEither.bimap(_ => DecodeError, data => (data, stat).some)
                }.recover {
                  case ZookeeperClientError(KeeperException.Code.NONODE) => none
                }
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
      mode: CreateMode
    ): F[Result[(Path, Stat)]] = {
      val absolutePath = transformRelative(path)
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
      createImpl(path, data, mode)

    def createEncode[T: ByteCodec](path: Path, obj: T, mode: CreateMode): F[Result[(Path, Stat)]] =
      for {
        encoded <- Sync[F].fromTry(ByteCodec[T].encode(obj))
        result  <- create(path, encoded, mode)
      } yield result

    def delete(path: Path, version: Int): F[Result[Unit]] = {
      val absolutePath = transformRelative(path)
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
      val absolutePath = transformRelative(path)
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
      val absolutePath = transformRelative(path)
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

    def exists(path: Path, watch: Boolean): F[Result[Option[Stat]]] = {
      val transformed = transformRelative(path)

      existsImpl { cb =>
        if (watch) {
          underlying.exists(transformed.raw, true, cb, Context.Empty)
        } else underlying.exists(transformed.raw, false, cb, Context.Empty)
      }
    }

    def exists[E[_]](path: Path, watch: Watcher[F])(implicit handler: Aux[F, E]): E[Result[Option[Stat]]] = {
      val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watch.process(event))
      val transformed             = transformRelative(path)

      val effect = existsImpl { cb =>
        underlying.exists(transformed.raw, unsafeWatcher, cb, Context.Empty)
      }
      handler.registerCleanUp(effect, transformed, WatcherType.Any, underlying, unsafeWatcher)

    }

    private[zookeeper] def existsImpl(register: StatCallback => Any): F[Result[Option[Stat]]] = {
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

        val _ = register(cb)
      }
    }

//    def existsResource(path: Path, watch: Watcher[F]): Resource[F, Result[Option[Stat]]] = {
//
//      val transformed             = rebaseOnRoot(path)
//      val unsafeWatcher: AWatcher = event => dispatcher.unsafeRunAndForget(watch.process(event))
//
//      Resource.make[F, Result[Option[Stat]]] {
//        Async[F].async_[Result[Option[Stat]]] { callback =>
//          val cb: StatCallback = (rc, _, context, stat) => {
//
//            callback(
//              Context
//                .decode(context)
//                .map { _ =>
//                  onSuccess(rc) {
//                    stat.some.asRight
//                  }.recover {
//                    case ZookeeperClientError(KeeperException.Code.NONODE) => none
//                  }
//                }
//                .toEither
//            )
//          }
//          underlying.exists(transformed.raw, unsafeWatcher, cb, Context.Empty)
//        }
//      } { _ =>
//        Async[F]
//          .async_[Result[Unit]] { callback =>
//            val cb: VoidCallback = (rc, _, context) => {
//              val result = Context
//                .decode(context)
//                .map { _ =>
//                  onSuccess(rc)(().asRight[Error])
//                }
//                .toEither
//
//              callback(result)
//            }
//            underlying.removeWatches(transformed.raw, unsafeWatcher, WatcherType.Any, false, cb, Context.Empty)
//          }
//          .rethrow
//      }
//    }
    def addWatcher(path: Path, watcher: Watcher[F], mode: AddWatchMode): Resource[F, Unit] = {

      val transformed             = transformRelative(path)
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

    def asAbsolute: Zookeeper[F] = new ZookeeperLive[F](underlying, dispatcher, config, false)

    private def transformRelative(path: Path): Path = if (relative) path.rebase(config.root) else path

    private def readPathName(path: Path): Path =
      path.extractSequential {
        _.split(serialSeparator) match {
          case Array(name, serial) => serial.toLongOption.map(value => SequentialContext(name, value))
          case _                   => None

        }

      }
  }
}

object syntax {
  implicit final class ZookeeperUnsafeOps[F[_]](private val zookeeper: Zookeeper[F]) extends AnyVal {
    def unsafeGetData[T: ByteCodec](path: Path, watch: Watcher[F])(implicit
      functor: Functor[F]
    ): F[Result[(T, Stat)]] =
      flattenOption(
        zookeeper
          .getData(path, watch)
      )
    def unsafeGetData[T: ByteCodec](path: Path, watch: Boolean)(implicit functor: Functor[F]): F[Result[(T, Stat)]] =
      flattenOption(
        zookeeper
          .getData(path, watch)
      )

    private def flattenOption[T](
      effect: F[Result[Option[(T, Stat)]]]
    )(implicit functor: Functor[F]): F[Result[(T, Stat)]] =
      effect.map(_.flatMap(_.fold(ZookeeperClientError(KeeperException.Code.NONODE).asLeft[(T, Stat)])(_.asRight)))

  }

  implicit final class ZookeeperResourceOps[F[_]](private val zookeeper: Zookeeper[F]) extends AnyVal {

    def existsR(path: Path, watcher: Watcher[F])(implicit
      handler: WatcherHandler.Aux[F, Resource[F, *]]
    ): Resource[F, Result[Option[Stat]]] = zookeeper.exists(path, watcher)
  }
}
