package dmk.scala.algebird

import com.twitter.algebird.Approximate
import com.twitter.algebird.CMS
import com.twitter.algebird.CMSHasherImplicits.CMSHasherLong
import com.twitter.algebird.CMSHasherImplicits.CMSHasherString
import com.twitter.algebird.CMSMonoid
import com.twitter.algebird.HyperLogLog.int2Bytes
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird.MinHasher
import com.twitter.algebird.MinHasher32
import com.twitter.algebird.TopCMS
import com.twitter.algebird.TopPctCMS
import com.twitter.algebird.TopPctCMSMonoid
import java.util.Arrays
import com.twitter.algebird.MinHashSignature

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

    minHash(2)
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
    val topPctCMSMonoid: TopPctCMSMonoid[String] = TopPctCMS.monoid[String](eps, delta, seed, heavyHittersPct)
    val r = (0 until num)
    val idPrefix = ""
    //    val idPrefix = "key:"
    val tmpKey = s"${idPrefix}4"

    val list = r.map { i =>
      val rNum = Math.round(Math.random() * num)
      topPctCMSMonoid.create(s"$idPrefix$rNum")
      //topPctCMSMonoid.create(rNum)
    }

    val topCMS = topPctCMSMonoid.sum(list)
    val approx = topCMS.frequency(tmpKey)
    println(s"$approx")
    // seems to work for strings
    val hh = topCMS.heavyHitters
    println(s"$hh")
  }

  def minHash(num: Int) = {
    println("\n===minHash")
    val numHashes = 50
    val targetThreshold = .8
    val numBands = MinHasher.pickBands(targetThreshold, numHashes)
    println(s"number of bands $numBands")
    val minHasher = new MinHasher32(numHashes, numBands)

    val prefix = "key:"
    val r = (0 until num)
    val doc1 = """Quantum mechanics is essential to understanding the behavior
    of systems at atomic length scales and smaller. """
    val doc2 = """The discovery that particles are discrete packets of energy 
    with wave-like properties led to the branch of physics dealing with atomic and subatomic systems which is today called quantum mechanics. """

    println(doc1)
    println(doc2)

    def normalizeDoc = { doc: String =>
      doc.split("\\s+").map { word =>
        word.replaceAll("[\\.\\?\\:',-_\\$]", "")
      }.map { word =>
        word.toLowerCase()
      }
    }

    val d1Tokens = normalizeDoc(doc1)
    val d2Tokens = normalizeDoc(doc2)

    val window = 2
    val d1Shingles = d1Tokens.sliding(window)
    val d2Shingles = d2Tokens.sliding(window)

    def printShingles = { shingles: Iterator[Array[String]] =>
      shingles.foreach { x => println(x.mkString(" ")) }
    }

    printShingles(d1Shingles)
    println("====")
    printShingles(d2Shingles)

    var sigs1 = d1Shingles.map { arr =>
      val shingle = arr.mkString(" ")
      minHasher.init(shingle)
    }
    println(sigs1)
    var doc1Sig = minHasher.sum(sigs1)
    
    var sigs2 = d2Shingles.map { arr =>
      val shingle = arr.mkString(" ")
      minHasher.init(shingle)
    }
    var doc2Sig = minHasher.sum(sigs2)
    
    var d = minHasher.similarity(doc1Sig, doc2Sig)
    println(s"doc1 and doc2 sim=$d")
//    def shinglesToHashBands = { shingles: Iterator[Array[String]] =>
//      shingles.flatMap { arr =>
//          val shingle = arr.mkString(" ")
//          val sig = minHasher.init(shingle)
//          val buckets = minHasher.buckets(sig)
//          buckets.map { bucket =>
//            (bucket, shingle)
//          }
//      }
//    }
//    val bands1 = shinglesToHashBands(d1Shingles)
//    .groupBy { _._1 }.map { case(a,b) => (a, b.map { _._2 }) }
//    val bands2 = shinglesToHashBands(d2Shingles)

//    println(bands1.mkString(" "))
//    println(bands2.mkString(" "))
    
    def shinglesToHashBands = { (shingles: Iterator[Array[String]], docId:String) =>
      shingles.flatMap { arr =>
          val shingle = arr.mkString(" ")
          println(shingle)
          val sig = minHasher.init(shingle)
          val buckets = minHasher.buckets(sig)
          buckets.map { case(bucket) =>
            (bucket, Set((docId, sig)))
          }
      }
    }
    val bands1 = shinglesToHashBands(d1Shingles, "doc1")
    val bands2 = shinglesToHashBands(d2Shingles, "doc2")
    val allBands = bands1 ++ bands2
    //allBands.foreach { println(_) }

//    val simList = allBands.flatMap { case (bucketTuple, sigSet) =>
//      println(s"$bucketTuple sigSet $sigSet")
//      for {
//        (id1, sig1) <- sigSet
//        (id2, sig2) <- sigSet
//        sim = minHasher.similarity(sig1, sig2)
//        if (id1 != id2 && sim >= targetThreshold)
//      } yield (id1, id2)
//    }
//     println(simList)
    
    val s1 = "how now brown cow"
    val s2 = "how say you now"
    val s3 = "nink in the sink"
    val sig1 = minHasher.init(s1)
    println(minHasher.approxCount(sig1.bytes))
    val sig2 = minHasher.init(s2)
    val sig3 = minHasher.init(s3)
    println(minHasher.approxCount(sig3.bytes))
    val sigSet1 = minHasher.sum(List(sig1, sig2))
    val sigSet2 = minHasher.sum(List(sig1, sig3))
    println(minHasher.approxCount(sigSet1.bytes))
    println(minHasher.approxCount(sigSet2.bytes))
    println(s"'$s1' and '$s2' similarity ${minHasher.similarity(sigSet1, sigSet2)}")
    println(s"'$s2' and '$s1' similarity ${minHasher.similarity(sigSet2, sigSet1)}")
    println(s"'$s1' and '$s1' similarity ${minHasher.similarity(minHasher.init(s1), minHasher.init(s1))}")
  }

}