package dmk.scala.algebird

import com.twitter.algebird.Approximate
import com.twitter.algebird.CMS
import com.twitter.algebird.CMSHasherImplicits.CMSHasherLong
import com.twitter.algebird.CMSHasherImplicits.CMSHasherString
import com.twitter.algebird.CMSMonoid
import com.twitter.algebird.HyperLogLog.int2Bytes
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.TopCMS
import com.twitter.algebird.TopPctCMS
import com.twitter.algebird.TopPctCMSMonoid
import com.twitter.algebird.MinHasher32
import com.twitter.algebird.MinHasher

object StreamingAlgorithms {

  def main(args: Array[String]) {
    StreamingAlgorithms.init
  }

  def init(): Unit = {
    val num = 50

    hllFewInts()
    hllLotsInts(num)
    hllLotsStrings(num)

    cms()
    cmsLotsOfLongs(num)
    cmsWithStrings(num)

    heavyHitters(num)

    minHash(num)
  }

  def hllFewInts(): Unit = {
    println("\n===hllFewInts")
    val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hlls = data.map { hllMonoid.create(_) }
    val combinedHLL = hllMonoid.sum(hlls)
    println(s"combinedHLL $combinedHLL")
    println(s"combinedHLL.estimatedSize ${combinedHLL.estimatedSize}")
  }

  def hllLotsInts(num: Int): Unit = {
    println("\n===hllLotsInts")
    val r = (0 until num)
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hlls = r.map { hllMonoid.create(_) }
    val combinedHLL = hllMonoid.sum(hlls)
    println(s"combinedHLL $combinedHLL")
    println(s"combinedHLL.estimatedSize ${combinedHLL.estimatedSize}")
  }

  def hllLotsStrings(num: Int): Unit = {
    println("\n===hllLotsStrings")
    val r = (0 until num)
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val keyPrefix = "key:"
    val hlls = r.map { i =>
      hllMonoid.create(s"${keyPrefix + i}".getBytes("UTF-8"))
    }
    val combinedHLL = hllMonoid.sum(hlls)
    println(s"combinedHLL $combinedHLL")
    println(s"combinedHLL.estimatedSize ${combinedHLL.estimatedSize}")
    println(s"${hllMonoid.sizeOf(combinedHLL)}")
  }

  def cms(): Unit = {
    println("\n===cms")
    // Creates a monoid for a CMS that can count `Long` elements.
    val eps = 0.001
    val delta = 1E-10
    val seed = 1
    val cmsMonoid: CMSMonoid[Long] = CMS.monoid[Long](eps, delta, seed)

    val id = 1L
    val cms: CMS[Long] = cmsMonoid.create(id)
    val estimate: Approximate[Long] = cms.frequency(id)
    println(s"estimate freq of $id is ${estimate}")
  }

  def cmsLotsOfLongs(num: Int): Unit = {
    println("\n===cmsLotsOfLongs")
    val eps = 0.001
    val delta = 1E-10
    val seed = 1
    val cmsMonoid: CMSMonoid[Long] = CMS.monoid[Long](eps, delta, seed)

    val id = 1L
    val r = (0 until num)
    val cmsList = r.map { i =>
      cmsMonoid.create(id)
    }

    val cms: CMS[Long] = cmsMonoid.sum(cmsList)
    val estimate: Approximate[Long] = cms.frequency(id)
    println(s"estimate freq of $id is ${estimate}, expected be around $num")
  }

  def cmsWithStrings(num: Int): Unit = {
    println("\n===cmsWithStrings")
    val eps = 0.001
    val delta = 1E-10
    val seed = 1
    val cmsMonoid: CMSMonoid[String] = CMS.monoid[String](eps, delta, seed)

    val id = "key:1"
    val r = (0 until num)
    val cmsList = r.map { i =>
      cmsMonoid.create(id)
    }

    val cms: CMS[String] = cmsMonoid.sum(cmsList)
    val estimate: Approximate[Long] = cms.frequency(id)
    println(s"estimate freq of $id is ${estimate}, expected to be around $num")
  }

  def heavyHitters(num: Int) = {
    println("\n===heavyHitters")
    val eps = 0.001
    val delta = 1E-10
    val seed = 1
    val heavyHittersPct = .05
    val topPctCMSMonoid: TopPctCMSMonoid[Long] = TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct)
    val r = (0 until num)
    val idPrefix = ""
    //    val idPrefix = "key:"
    val tmpKey = s"${idPrefix}4"

    val list = r.map { i =>
      val rNum = Math.round(Math.random() * num)
      //      topPctCMSMonoid.create(s"$idPrefix$rNum")
      topPctCMSMonoid.create(rNum)
    }

    val topCMS = topPctCMSMonoid.sum(list)
    val approx = topCMS.frequency(4)
    println(s"$approx")
    // empty set for > ~200 elements?
    // seems to work for strings
    val hh = topCMS.heavyHitters
    println(s"$hh")
  }

  def minHash(num: Int) = {
    println("\n===minHash")
    val numHashes = 50
    val targetThreshold = .8
    val numBands = MinHasher.pickBands(targetThreshold, numHashes)
    val minHasher = new MinHasher32(numHashes, numBands)

    val prefix = "key:"
    val r = (0 until num)
    val simMap = r.flatMap { i =>
      val rNum = Math.round(Math.random() * num)
      //      val str = s"$prefix$rNum"
      val str = s"$prefix$i"
      val sig = minHasher.init(str)
      val buckets = minHasher.buckets(sig)
      buckets.map { bucket =>
        (bucket, Seq(str))
      }
    }

//    val grouped = simMap.groupBy { _._1 }
//    grouped.foreach {
//      case (a, b) =>
//        println(a + " num elements " + b.size)
//    }
//    println(grouped)

    val s1 = "how now brown cow"
    val s2 = "how say you now"
    val s3 = "nink in the sink"
    val sig1 = minHasher.init(s1)
    val sig2 = minHasher.init(s2)
    val sig3 = minHasher.init(s3)
    val sigSet1 = minHasher.sum(List(sig1, sig2))
    val sigSet2 = minHasher.sum(List(sig1, sig3))
    println(minHasher.approxCount(sigSet1.bytes))
    println(minHasher.approxCount(sigSet2.bytes))
    println(minHasher.similarity(sigSet1, sigSet2))
    println(minHasher.similarity(minHasher.init(s1), minHasher.init(s1)))
  }

}