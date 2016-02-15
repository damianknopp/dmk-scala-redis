package dmk.scala.jedis

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

/**
 * Start 1 redis master, running on the default port 6379
 * <ol>
 *  <li>./src/redis-server --loglevel verbose</li>
 *  <li>optionally, connect a client ./src/redis-cli</li>
 * </ol>
 */

object SimpleJedisClient {

  val host = "localhost"
  val port = 6379

  def main(args: Array[String]) {
    println("simple redis client")
    SimpleJedisClient.init
  }

  def init(): Unit = {
    val pool = createJedisPool
    try {
      keyval(pool)
      hll(pool)
      writeDataPipeline(pool, 10000)
    } finally {
      if (pool != null) {
        pool.destroy()
      }
    }
  }

  def keyval(pool: JedisPool): Unit = {
    println("key value")
    val jedis = pool.getResource
    try {
      val key = "foo"
      val expireSecs = 1
      jedis.set(key, "bar", "NX", "PX", expireSecs)
      val value = jedis.get(key)
      println(s"key foo has value: $value")

      val ttl = jedis.ttl(key)
      println(s"ttl is $ttl")
      Thread.sleep(expireSecs * 1000)
      val exists = jedis.exists(key)
      if (!exists) {
        println(s"$key has expired")
      } else {
        println(s"$key has not expired!, expected to expire in ${jedis.ttl(key)}")
      }
    } finally {
      if (jedis != null) {
        jedis.close
      }
    }

  }

  def writeDataPipeline(pool: JedisPool, num: Int): Unit = {
    println("write datapipeline")
    val jedis = pool.getResource
    val pipeline = jedis.pipelined()
    try {
      val res = pipeline.multi()
      val expireSecs = 60 * 5
      val keyPrefix = "key:"
      val r = (0 until num)
      r.foreach { i =>
        val key = s"$keyPrefix$i"
        println(s"setting $key")
        pipeline.set(key, s"val-$i", "NX", "EX", expireSecs)
      }
      pipeline.exec()

      pipeline.multi()
      r.foreach { i => 
        val key = s"$keyPrefix$i"
        val value = pipeline.get(key).toString()
        println(s"$key $value")
      }
      pipeline.exec()
    } finally {
      if (pipeline != null) {
        pipeline.clear()
        pipeline.close()
      }
      if (jedis != null) {
        jedis.close
      }
    }

  }

  def hll(pool: JedisPool): Unit = {
    println("hyperloglog")
    val jedis = pool.getResource
    try {
      // set HLL
      val name = "hll"
      jedis.pfadd(name, "a", "b", "c", "d", "e")
      jedis.pfadd(name, "1", "2", "3", "4")
      val count = jedis.pfcount(name)
      println(s"hll has $count elements")
    } finally {
      if (jedis != null) {
        jedis.close
      }
    }
  }

  @Deprecated
  private def createNonPooledJedisConnection(): Jedis = {
    val jedis = new Jedis(host, port)
    println(s"created new jedis connection: $jedis")
    jedis
  }

  private def createJedisPool: JedisPool = {
    new JedisPool(host, port)
  }
}