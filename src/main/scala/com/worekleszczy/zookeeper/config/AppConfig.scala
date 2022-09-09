package com.worekleszczy.zookeeper.config

case class AppConfig(
  id: String,
  zookeeper: Zookeeper
)

case class Zookeeper(host: String, port: Int)
