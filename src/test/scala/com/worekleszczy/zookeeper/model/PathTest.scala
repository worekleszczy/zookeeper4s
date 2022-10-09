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
    assertEquals(result.get.sequential, None)
  }

  test("should create a path object from a valid path ending with slash") {

    val testPath = "/bacchus/lock-1/"

    val result = Path.apply(testPath)

    assertEquals(
      result.get.value.toString,
      "/bacchus/lock-1"
    )
    assertEquals(result.get.sequential, None)

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
    assertEquals(result.sequential, None)

  }

  test("should return valid grandparent for a valid path") {
    val testPath = "/bacchus/lock-1/node-1"

    val result = Path.unsafeFromString(testPath)

    assertEquals(result.parent.parent.value.toString, "/bacchus")
    assertEquals(result.sequential, None)

  }

  test("should return a fileName for a valid path") {

    val testPath = "/bacchus/lock-1"

    val result = Path.unsafeFromString(testPath)

    assertEquals(result.name, "lock-1")
    assertEquals(result.sequential, None)

  }

  test("should rebase a path on top another path") {
    val root = Path.unsafeFromString("/bacchus")

    val relativePath = Path.unsafeFromString("/lock-1")

    val result = relativePath.rebase(root)

    assertEquals(result.value.toString, "/bacchus/lock-1")
    assertEquals(result.sequential, None)

  }

  test("should return root object if slash rebased on root") {
    val root = Path.unsafeFromString("/bacchus")

    val relativePath = Path.unsafeFromString("/")

    val result = relativePath.rebase(root)

    assertEquals(result.value.toString, "/bacchus")
    assertEquals(result.sequential, None)

  }
//  /*
//    Path: /bacchus/another_bites
//    Name: /bacchus/another_bites0000000027
//   */
//  test("should extract sequential number if name starts with a path") {
//
//    val path = Path.fromPathAndName("/bacchus/another_bites", "/bacchus/another_bites0000000027").get
//
//    assertEquals(path.value.toString, "/bacchus/another_bites0000000027")
//    assertEquals(path.sequential, Some(27L))
//  }
//
//  test("should fail if name does not stars with a path") {
//
//    val path = Path.fromPathAndName("/bacchus/other", "/bacchus/another_bites0000000027")
//
//    assertEquals(path.failed.get, PathPrefixException("/bacchus/other", "/bacchus/another_bites0000000027"))
//  }

//  test("should fail if serial is not a number") {
//
//    val path = Path.fromPathAndName("/bacchus/another_bites", "/bacchus/another_bitesxoxoxox")
//
//    assertEquals(path.failed.get, InvalidSequentialNumber("xoxoxox"))
//  }

  test("should strip base of path") {

    val base         = Path.unsafeFromString("/bacchus")
    val prefixedPath = Path.unsafeFromString("/bacchus/another_bites")

    val result = prefixedPath.stripBase(base)

    assertEquals(result.value.toString, "/another_bites")
    assertEquals(result.sequential, None)
  }

  test("should keep sequential number when stripping base of path") {

    val base         = Path.unsafeFromString("/bacchus")
    val prefixedPath = Path.apply("/bacchus/another:10000", SequentialContext("another", 10000L)).get

    val result = prefixedPath.stripBase(base)

    assertEquals(result.value.toString, "/another:10000")
    assertEquals(result.sequential, Some(10000L))
  }

  test("should leave prefix untouched if path doesn't start with a base") {

    val base         = Path.unsafeFromString("/bacchus2")
    val prefixedPath = Path.unsafeFromString("/bacchus/another_bites")

    val result = prefixedPath.stripBase(base)

    assertEquals(result.value.toString, "/bacchus/another_bites")
    assertEquals(result.sequential, None)
  }

  test("should leave prefix unaltered if base is slash") {

    val base         = Path.unsafeFromString("/")
    val prefixedPath = Path.unsafeFromString("/bacchus/another_bites")

    val result = prefixedPath.stripBase(base)

    assertEquals(result.value.toString, "/bacchus/another_bites")
    assertEquals(result.sequential, None)
  }

  test("should have level 0 for root node") {
    assertEquals(Path.root.level, 0)
  }

  test("should have level 1 for node under root") {
    assertEquals(Path.unsafeFromString("/test-1").level, 1)
  }

  test("should have level 1 for node under root with trailing slash") {
    assertEquals(Path.unsafeFromString("/test-1/").level, 1)
  }

  test("should transform path name") {

    val sequentialContext = SequentialContext("name", 100)

    val base   = Path("/bacchus/test-1/", sequentialContext).get
    val result = base.transformFileName(_ + ":")
    assertEquals(result.name, "test-1:")
    assertEquals(result.sequential, None)
  }

  test("should resolve child name") {
    val sequentialContext = SequentialContext("name", 100)

    val base   = Path("/bacchus/test-1/", sequentialContext).get
    val result = base.resolve("inner")
    assertEquals(result.raw, "/bacchus/test-1/inner")
    assertEquals(result.sequential, None)
  }

  test("should resolve child if it starts with slash") {
    val base   = Path("/bacchus/test-1/", SequentialContext("name", 100)).get
    val result = base.resolve("/inner")
    assertEquals(result.raw, "/bacchus/test-1/inner")
    assertEquals(result.sequential, None)
  }

  test("should resolve child if it starts and ends with slash") {
    val base   = Path("/bacchus/test-1/", SequentialContext("name", 100)).get
    val result = base.resolve("/inner/")
    assertEquals(result.raw, "/bacchus/test-1/inner")
    assertEquals(result.sequential, None)
  }

  test("should extract sequential from name") {
    val base = Path("/bacchus/test_1000").get
    val result = base.extractSequential(_.split('_') match {
      case Array(name, seq) => seq.toLongOption.map(SequentialContext(name, _))
      case _                => None
    })

    assertEquals(result.raw, "/bacchus/test_1000")
    assertEquals(result.sequential, Some(1000L))
    assertEquals(result.name, "test")
  }

  test("should not extract sequential from name") {
    val base = Path("/bacchus/test:1000").get
    val result = base.extractSequential(_.split('_') match {
      case Array(name, seq) => seq.toLongOption.map(SequentialContext(name, _))
      case _                => None
    })
    assertEquals(result.raw, "/bacchus/test:1000")
    assertEquals(result.sequential, None)
  }

  test("sorting paths with sequential number from min to max") {

    val a = Path("/a", SequentialContext("a", 3L)).get
    val b = Path("/b", SequentialContext("b", 2L)).get
    val c = Path("/c", SequentialContext("c", 1L)).get

    assertEquals(Vector(a, b, c).sorted, Vector(c, b, a))

  }

  test("put paths without sequential number last") {

    val a = Path("/a").get
    val d = Path("/d").get
    val b = Path("/b", SequentialContext("b", 2L)).get
    val c = Path("/c", SequentialContext("c", 1L)).get

    assertEquals(Vector(a, d, b, c).sorted, Vector(c, b, a, d))

  }

}
