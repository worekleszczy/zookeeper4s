package com.worekleszczy.zookeeper

import cats.Applicative
import org.apache.zookeeper.WatchedEvent

trait Watcher[F[_]] {
  def process(event: WatchedEvent): F[Unit]

  final def filterWatch(implicit applicative: Applicative[F]): Watcher[F] = {
    case event if event.getType.getIntValue >= 5 => applicative.unit
    case other                                   => process(other)
  }
}

object Watcher {

  def instance[F[_]](handle: WatchedEvent => F[Unit]): Watcher[F] = event => handle(event)

  def partial[F[_]: Applicative](handle: PartialFunction[WatchedEvent, F[Unit]]): Watcher[F] =
    event => handle.lift(event).getOrElse(Applicative[F].pure(()))

  /**
    * Constructs watcher that will again register itself if handle doesn't match passed event
    * @param unmatchedFunction action what should be executed if handle doesn't match triggered event. Can be used
    *                          to register this watcher again.
    * @param handle partial function handling events
    * @tparam F effect type
    * @return
    */
  def partialOrElse[F[_]](
    unmatchedFunction: (WatchedEvent, Watcher[F]) => F[Unit]
  )(handle: PartialFunction[WatchedEvent, F[Unit]]): Watcher[F] = {
    lazy val watcher: Watcher[F] = event => handle.lift(event).getOrElse(unmatchedFunction(event, watcher))

    watcher
  }
}
