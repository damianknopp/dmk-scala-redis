package dmk.scala.algebird

import com.twitter.algebird.HyperLogLog.int2Bytes
import com.twitter.algebird.HyperLogLogMonoid

object StreamingAlgorithms {

  def main(args: Array[String]) {
    StreamingAlgorithms.init
  }

  def init(): Unit = {
    hllFewInts()
    val num = 10000
    hllLotsInts(num)
    hllLotsStrings(num)
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
  }
}