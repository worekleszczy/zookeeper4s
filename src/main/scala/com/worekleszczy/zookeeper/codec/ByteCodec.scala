package com.worekleszczy.zookeeper.codec

import java.nio.{ByteBuffer, CharBuffer}
import java.nio.charset.Charset
import scala.util._

trait ByteCodec[T] {

  def encode(value: T): Try[Array[Byte]]

  def decode(raw: Array[Byte]): Try[T]

}

object ByteCodec {

  private val utf8Charset = Charset.forName("UTF-8")

  implicit val utf8StringCodec: ByteCodec[String] = new ByteCodec[String] {

    def encode(value: String): Try[Array[Byte]] = {
      val encoder = utf8Charset.newEncoder()
      val buffer  = CharBuffer.wrap(value)

      Try(encoder.encode(buffer).array())
    }

    def decode(raw: Array[Byte]): Try[String] = {
      val decoder = utf8Charset.newDecoder()

      val buffer = ByteBuffer.wrap(raw)

      Try(decoder.decode(buffer).toString)
    }
  }

  def apply[T: ByteCodec]: ByteCodec[T] = implicitly[ByteCodec[T]]
}
