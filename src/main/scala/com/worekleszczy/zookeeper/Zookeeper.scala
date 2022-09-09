package com.worekleszczy.zookeeper

import com.worekleszczy.zookeeper.model.Path

trait Zookeeper[F[_]] {

  def watch(path: Path): F[Array[Byte]]

}
