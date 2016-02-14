package dmk.scala.jedis.masterslave

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool

/**
 * This program writes key values to the master, and reads from the slaves
 * 
 * To Run; start 2 redis masters, the first running on the default port 6379, the second on 6380
 * Connect to master 2 and issue the slaveof command
 * 
 * <ol>
 *  <li>./src/redis-server --loglevel verbose</li>
 *  <li>./src/redis-server --port 6380 --loglevel verbose</li>
 *  <li>optionally, connect a client to master 1, ./src/redis-cli</li>
 *  <li>connect a client to master 2 ./src/redis-cli -p 6380, type "slaveof localhost 6379</li>
 * </ol>
 */

object SimpleMasterSlaveJedisClient {

  val host = "localhost"
  val port = 6379
  val slave1Host = host
  val slave1Port = 6380

  def main(args: Array[String]) {
    println("simple master slave redis client")
    SimpleMasterSlaveJedisClient.init
  }

  def init(): Unit = {
    val masterPool = createJedisPoolForMaster
    try {
      writeData(masterPool, 1000)
    } finally {
      if (masterPool != null) {
        masterPool.destroy()
      }
    }
    
    val slavePool = createJedisPoolForSlaves
    try {
      readData(slavePool, 1000)
    } finally {
      if (slavePool != null) {
        slavePool.destroy()
      }
    }
  }

  def writeData(pool: JedisPool, num: Int): Unit = {
    println(s"writing $num elements")
    val jedis = pool.getResource
    try {
      (0 until num).foreach { i =>
        val key = s"key-$i"
        val value = s"value-$i"
        jedis.set(key, value)
        println(s"writing $key : $value")
      }
    } finally {
      if (jedis != null) {
        jedis.close
      }
    }

  }
  
  def readData(pool: JedisPool, num: Int): Unit = {
    println("reading $num elements")
        val jedis = pool.getResource
    try {
      (0 until num).foreach { i =>
        val key = s"key-$i"
        val value = jedis.get(key)
        println(s"read $key : $value")
      }
    } finally {
      if (jedis != null) {
        jedis.close
      }
    }
  }

  private def createJedisPoolForMaster: JedisPool = {
    new JedisPool(host, port)
  }

  private def createJedisPoolForSlaves: JedisPool = {
    new JedisPool(slave1Host, slave1Port)
  }

}