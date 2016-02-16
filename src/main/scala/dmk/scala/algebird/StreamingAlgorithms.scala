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

object StreamingAlgorithms {

  def main(args: Array[String]) {
    StreamingAlgorithms.init
  }

  def init(): Unit = {
    val num = 10000

    hllFewInts()
    hllLotsInts(num)
    hllLotsStrings(num)
    
    cms() 
    cmsLotsOfLongs(num)
    cmsWithStrings(num)
    
    heavyHitters(num)
  }

  def hllFewInts(): Unit = {
    val data = List(1, 1, 2, 2, 3, 3, 4, 4, 5, 5)
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hlls = data.map { hllMonoid.create(_) }
    val combinedHLL = hllMonoid.sum(hlls)
    println(s"combinedHLL $combinedHLL")
    println(s"combinedHLL.estimatedSize ${combinedHLL.estimatedSize}")
  }
  
  def hllLotsInts(num: Int): Unit = {
    val r = (0 until num)
    val hllMonoid = new HyperLogLogMonoid(bits = 12)
    val hlls = r.map { hllMonoid.create(_) }
    val combinedHLL = hllMonoid.sum(hlls)
    println(s"combinedHLL $combinedHLL")
    println(s"combinedHLL.estimatedSize ${combinedHLL.estimatedSize}")
  }
  
  def hllLotsStrings(num: Int): Unit = {
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
    println(s"estimate freq of $id is ${estimate}, expected be around $num")
  }
  
  def heavyHitters(num: Int) = {
    val eps = 0.001
    val delta = 1E-10
    val seed = 1
    val heavyHittersPct = 0.1
    val topPctCMSMonoid: TopPctCMSMonoid[String] = TopPctCMS.monoid[String](eps, delta, seed, heavyHittersPct)
    val r = (0 until num)
    val idPrefix = "key:"
//    val list1 = r.map { i => 
//      topPctCMSMonoid.create(s"$idPrefix50")
//    }
    
    val list = r.map { i => 
      val rNum = Math.random() * num
      topPctCMSMonoid.create(s"$idPrefix$rNum")
    }
    
    val topCMS = topPctCMSMonoid.sum(list)
    println(s"${topCMS.frequency("key:4")}")
    println(s"${topCMS.heavyHitters}")
  }
}