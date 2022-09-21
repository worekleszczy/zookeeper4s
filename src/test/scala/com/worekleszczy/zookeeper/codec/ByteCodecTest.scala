package com.worekleszczy.zookeeper.codec

import com.worekleszczy.zookeeper.codec.ByteCodec.syntax._
import munit.FunSuite

import scala.util.Success

class ByteCodecTest extends FunSuite {

  test("Decode same value after encoding") {
    val value = "alamakotaakotmaale"

    assertEquals(value.encode.flatMap(_.decode[String]), Success(value))
  }
}
