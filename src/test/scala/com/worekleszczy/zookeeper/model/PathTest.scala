package com.worekleszczy.zookeeper.model

import com.worekleszczy.zookeeper.model.Path.InvalidPathException
import munit.FunSuite

class PathTest extends FunSuite {

  test("should create a path object from a valid path") {

    val testPath = "/bacchus/lock-1"

    val result = Path.apply(testPath)

    assertEquals(
      result.get.value.toString,
      "/bacchus/lock-1"
    )
  }

  test("should create a path object from a valid path ending with slash") {

    val testPath = "/bacchus/lock-1/"

    val result = Path.apply(testPath)

    assertEquals(
      result.get.value.toString,
      "/bacchus/lock-1"
    )
  }

  test("should fail for a relative path") {

    val testPath = "bacchus/lock-1"

    val result = Path.apply(testPath)

    assertEquals(
      result.failed.get,
      InvalidPathException(testPath)
    )
  }

  test("should return valid parent for a valid path") {
    val testPath = "/bacchus/lock-1"

    val result = Path.unsafeFromString(testPath)

    assertEquals(result.parent.value.toString, "/bacchus")
  }

  test("should return valid grandparent for a valid path") {
    val testPath = "/bacchus/lock-1/node-1"

    val result = Path.unsafeFromString(testPath)

    assertEquals(result.parent.parent.value.toString, "/bacchus")
  }

  test("should return a fileName for a valid path") {

    val testPath = "/bacchus/lock-1"

    val result = Path.unsafeFromString(testPath)

    assertEquals(result.name, "lock-1")
  }

  test("should rebase a path on top another path") {
    val root = Path.unsafeFromString("/bacchus")

    val relativePath = Path.unsafeFromString("/lock-1")

    val result = relativePath.rebase(root)

    assertEquals(result.value.toString, "/bacchus/lock-1")
  }

  test("should return root object if slash rebased on root") {
    val root = Path.unsafeFromString("/bacchus")

    val relativePath = Path.unsafeFromString("/")

    val result = relativePath.rebase(root)

    assertEquals(result.value.toString, "/bacchus")
  }
}
