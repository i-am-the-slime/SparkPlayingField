package org.menthal

import org.joda.time.DateTime
import org.menthal.aggregations.tools.WindowFunctions
import org.menthal.io.postgres.PostgresDump
import org.menthal.model.events._
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}

import scala.io.Source

/**
 * Created by konrad on 15/04/15.
 */
class MedianTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  "findMedianSort " should "find median corectly for odd number of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 1L)
    val median = WindowFunctions.findMedianSort(elements)
    median shouldBe 3.0
  }

  it should "find median corectly for even nuber of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 2L, 1L)
    val median = WindowFunctions.findMedianSort(elements)
    median shouldBe 2.5
  }

  "findMedian with Hoare aglorithm" should "fid median corectly for odd number of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 1L)
    val median = WindowFunctions.findMedian(elements)
    median shouldBe 3.0
  }

  it should "be find median corectly for even nuber of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 2L, 1L)
    val median = WindowFunctions.findMedian(elements)
    median shouldBe 2.5
  }

  "median with histogram aglorithm" should "find median corectly for odd number of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 1L)
    val median = WindowFunctions.findMedianHistogram(elements)
    median shouldBe 3L
  }

  it should "find median corectly for even nuber of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 2L, 1L)
    val median = WindowFunctions.findMedianHistogram(elements)
    median shouldBe 2.5.toLong
  }

  "median algorithm" should "find median corectly for odd number of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 1L)
    val median = WindowFunctions.findMedianHistogram(elements)
    median shouldBe 3L
  }

  it should "find median corectly for even nuber of elements" in {
    val elements = Array(5L, 3L, 4L, 2L, 2L, 1L)
    val median = WindowFunctions.findMedianHistogram(elements)
    median shouldBe 2.5.toLong
  }

  "medianFilter" should "compute median of windowds for nonempty iterbable" in {
    val signal = List(1L,1L,1L,2L,2L,3L,4L,4L,5L,5L,5L,6L)
    val windowSize = 3
    val expected = List(1L,1L,1L,2L,2L,3L,4L,4L,5L,5L,5L,6L)
    val filtered = WindowFunctions.medianFilter(signal)
  }


}
