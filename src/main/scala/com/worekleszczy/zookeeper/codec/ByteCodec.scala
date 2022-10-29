package com.worekleszczy.zookeeper.codec

import java.nio.charset.Charset
import java.nio.{ByteBuffer, CharBuffer}
import java.util
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

      Try {
        val encoded = encoder.encode(buffer)
        util.Arrays.copyOf(encoded.array(), encoded.limit())
      }
    }

    def decode(raw: Array[Byte]): Try[String] = {
      val decoder = utf8Charset.newDecoder()

      val buffer = ByteBuffer.wrap(raw)

      Try(decoder.decode(buffer).toString)
    }
  }

  implicit val intCodec: ByteCodec[Int] = new ByteCodec[Int] {
    def encode(value: Int): Try[Array[Byte]] = utf8StringCodec.encode(String.valueOf(value))

    def decode(raw: Array[Byte]): Try[Int] = utf8StringCodec.decode(raw).flatMap(str => Try(str.toInt))
  }

  implicit val longCoded: ByteCodec[Long] = new ByteCodec[Long] {
    def encode(value: Long): Try[Array[Byte]] = utf8StringCodec.encode(String.valueOf(value))

    def decode(raw: Array[Byte]): Try[Long] = utf8StringCodec.decode(raw).flatMap(str => Try(str.toLong))
  }

  def apply[T: ByteCodec]: ByteCodec[T] = implicitly[ByteCodec[T]]

  object syntax {
    implicit final class EncodeOpts[T](private val value: T) extends AnyVal {
      def encode(implicit byteCodec: ByteCodec[T]): Try[Array[Byte]] = ByteCodec[T].encode(value)
    }

    implicit final class DecodeOpts(private val value: Array[Byte]) extends AnyVal {
      def decode[T](implicit byteCodec: ByteCodec[T]): Try[T] = ByteCodec[T].decode(value)
    }
  }
}
