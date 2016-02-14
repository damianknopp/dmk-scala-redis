package dmk.scala.jedis.shard

import java.util.Arrays
import java.util.List
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisShardInfo
import redis.clients.jedis.ShardedJedis
import redis.clients.jedis.ShardedJedisPool
import redis.clients.jedis.JedisPoolConfig

/**
 * This program writes data to redis master servers, the data is written as shards, based on the 
 * data hashing algorithm of the jedis shard client
 * 
 * To run, start 2 redis masters
 * The first running on the default port, 6379
 * The second running on port 6380
 * <ol>
 *  <li>./src/redis-server --loglevel verbose</li>
 *  <li>./src/redis-server --port 6380 --loglevel verbose</li>
 *  <li>optionally, connect a client to master 1, ./src/redis-cli</li>
 *  <li>optionally, connect a client to master 2 ./src/redis-cli -p 6380</li>
 * </ol>
 */
object JedisShardClient {

  val host1 = "localhost"
  val host2 = host1
  val port1 = 6379
  val port2 = 6380
  val SHARD_KEY_PREFIX = "SHARDKEY"

  def main(args: Array[String]) {
    println("sharded redis client")
    JedisShardClient.init
  }

  private def init(): Unit = {
    val jedisPool = createJedisShardConnectionPool
    val num = 10

    var jedisConnection: ShardedJedis = null
    try {
      jedisConnection = jedisPool.getResource()
      insertKeyVals(jedisConnection, num)
    } finally {
      if (jedisConnection != null) {
        jedisConnection.close()
      }
    }

    readShard1(num)
    readShard2(num)

    try {
      jedisConnection = jedisPool.getResource()
      readAll(jedisConnection, num)
    } finally {
      if (jedisConnection != null) {
        jedisConnection.close()
      }
    }

    jedisPool.destroy()

  }

  private def insertKeyVals(shardedJedis: ShardedJedis, num: Int): Unit = {
    val range = 0 until num
    range.foreach { i =>
      shardedJedis.set(s"$SHARD_KEY_PREFIX-$i", s"SHARDVAL-$i")
    }
  }

  private def readShard1(num: Int): Unit = {
    val shard = new Jedis(host1, port1)
    println("=====reading from shard1 only")
    (0 until num).foreach { i =>
      val value = shard.get(s"$SHARD_KEY_PREFIX-$i")
      if (value != null)
        println(s"$value")
    }
  }

  private def readShard2(num: Int): Unit = {
    val shard = new Jedis(host2, port2)
    println("=====reading from shard2 only")
    (0 until num).foreach { i =>
      val value = shard.get(s"$SHARD_KEY_PREFIX-$i")
      if (value != null)
        println(s"$value")
    }
  }

  private def readAll(sharedJedis: ShardedJedis, num: Int): Unit = {
    println("=====reading from all shards")
    (0 until num).foreach { i =>
      val value = sharedJedis.get(s"$SHARD_KEY_PREFIX-$i")
      if (value != null)
        println(s"$value")
    }
  }

  @Deprecated
  private def createJedisShardConnection(): ShardedJedis = {
    new ShardedJedis(createShardList())
  }

  private def createJedisShardConnectionPool(): ShardedJedisPool = {
    new ShardedJedisPool(new JedisPoolConfig(), createShardList())
  }

  private def createShardList(): List[JedisShardInfo] = {
    val shard1 = new JedisShardInfo(host1, port1)
    val shard2 = new JedisShardInfo(host2, port2)
    Arrays.asList(shard1, shard2)
  }
}