package com.worekleszczy.zookeeper

import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}

import scala.io.StdIn

object SimpleWatcher extends Watcher {
  def process(event: WatchedEvent): Unit = {
    println(event)
  }
}

/**
  * This is just to test zookeeper connection and API
  */
object UnsafeMain extends App {

  val zookeeper = new ZooKeeper("localhost:2181", 3000, SimpleWatcher)

  val children = zookeeper.getChildren("/bacchus", true)
  println(children)

  StdIn.readLine()

//  // default watcher is used
//  zookeeper.addWatch("/test", AddWatchMode.PERSISTENT)
//  zookeeper.addWatch("/test", AddWatchMode.PERSISTENT)
//  zookeeper.getChildren()
//
//  zookeeper.exists()
//
//  StdIn.readLine()

}
