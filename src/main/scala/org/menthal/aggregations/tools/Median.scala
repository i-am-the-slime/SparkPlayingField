package org.menthal.aggregations.tools

import scala.annotation.tailrec

/**
 * Created by konrad on 30/03/15.
 */
object WindowFunctions {

  def findMedianHistogram(s: Traversable[Long]) = {
    def medianHistogram(s: Traversable[Long], discarded: Int, medianIndex: Int): Long = {
      // The buckets
      def numberOfBuckets = (math.log(s.size).toInt + 1) max 2
      val buckets = new Array[Int](numberOfBuckets)

      // The upper limit of each bucket
      val max = s.max
      val min = s.min
      val increment = (max - min) / numberOfBuckets
      val indices = (-numberOfBuckets + 1 to 0) map (max + increment * _)

      // Return the bucket a number is supposed to be in
      def bucketIndex(d: Double) = indices indexWhere (d <=)

      // Compute how many in each bucket
      s foreach { d => buckets(bucketIndex(d)) += 1 }

      // Now make the buckets cumulative
      val partialTotals = buckets.scanLeft(discarded)(_+_).drop(1)

      // The bucket where our target is at
      val medianBucket = partialTotals indexWhere (medianIndex <)

      // Keep track of how many numbers there are that are less
      // than the median bucket
      val newDiscarded = if (medianBucket == 0) discarded else partialTotals(medianBucket - 1)

      // Test whether a number is in the median bucket
      def insideMedianBucket(d: Long) = bucketIndex(d) == medianBucket

      // Get a view of the target bucket
      val view = s.view filter insideMedianBucket

      // If all numbers in the bucket are equal, return that
      if (view forall (view.head ==)) view.head
      // Otherwise, recurse on that bucket
      else medianHistogram(view, newDiscarded, medianIndex)
    }
    medianHistogram(s, 0, (s.size - 1) / 2)
  }


  @tailrec def findKElem(arr: Array[Long], k:Int)(implicit choosePivot: Array[Long] => Long): Long = {
    val el = choosePivot(arr)
    val (lower, upper) = arr.partition(_ > el)
    val l = lower.size
    if (l == k) el
    else if (l == 0) {
      val (equal, larger) = arr.partition(_ == el)
      val e = equal.size
      if (e > k) el
      else findKElem(larger, k - e)
    }
    else if (l > k) findKElem(lower, k)
    else findKElem(upper, k - l)
  }

  def chooseRandomPivot(arr: Array[Long]): Long = arr(scala.util.Random.nextInt(arr.size))

  def findMedian(arr: Array[Long]):Double = {
    val l = arr.size
    implicit val pivot:Array[Long] => Long  = chooseRandomPivot
    if (l % 2 == 0) (findKElem(arr, l/2 - 1) + findKElem(arr, l/2))/2.0 else findKElem(arr, l/2).toDouble
  }

  def findMedianSort(arr: Array[Long]):Double = {
    val (lower, upper) = arr.sorted.splitAt(arr.size / 2)
    if (arr.size % 2 == 0) (lower.last + upper.head) / 2.0 else upper.head
  }

  def median(vals: Iterable[Long]):Long = findMedianHistogram(vals)


  def windowFilter[A](windowFunction: Iterable[Long] => A, filterWindow: Int)(s:Iterable[Long]): List[A] = {
    def updateBuckets(buckets: List[List[Long]], el: Long): (A,List[List[Long]]) = {
      val updatedBuckets = List(el) :: buckets.map(el :: _)
      val finishedWindow = updatedBuckets.last
      val windowValue = windowFunction(finishedWindow)
      (windowValue, updatedBuckets take (filterWindow - 1))
    }

    @tailrec
    def windowFilterRec(s:Iterable[Long], buckets: List[List[Long]], results:List[A]):List[A] = {
      s match {
        case x::xs =>
          val (filteredVal, updatedBuckets) = updateBuckets(buckets, x)
          val updatedResults = filteredVal :: results
          windowFilterRec(xs, updatedBuckets, updatedResults)
        case Nil =>
          //We have to move results forwards by filterWindow/2
          val unfinishedMedians = buckets.reverse take filterWindow/2 map windowFunction//CHECK
          val fullResults = unfinishedMedians.reverse ::: results
          fullResults.reverse drop filterWindow/2
      }
    }
    windowFilterRec(s, Nil, Nil)
  }

  val medianFilterWindow = 5
  def medianFilter:Iterable[Long] => List[Long] = windowFilter[Long](median, medianFilterWindow)

}
