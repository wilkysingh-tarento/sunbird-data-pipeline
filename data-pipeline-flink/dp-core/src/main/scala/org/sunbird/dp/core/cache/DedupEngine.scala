package org.sunbird.dp.core.cache

import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisException}


class DedupEngine(redisConnect: RedisConnect, store: Int, expirySeconds: Int) extends Serializable {

  private val serialVersionUID = 6089562751616425354L
  private[this] var redisConnection: Jedis = redisConnect.getConnection
  redisConnection.select(store)

  @throws[JedisException]
  @throws[JedisConnectionException]
  def isUniqueEvent(checksum: String): Boolean = {
    var unique = false
    try {
      unique = !redisConnection.exists(checksum)
    } catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        ex.printStackTrace()
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(this.store, backoffTimeInMillis = 10000)
        unique = !this.redisConnection.exists(checksum)
    }
    unique
  }

  @throws[JedisException]
  @throws[JedisConnectionException]
  def storeChecksum(checksum: String): Unit = {
    try
      redisConnection.setex(checksum, expirySeconds, "")
    catch {
      case ex@(_: JedisConnectionException | _: JedisException) =>
        ex.printStackTrace()
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(this.store, backoffTimeInMillis = 10000)
        this.redisConnection.select(this.store)
        this.redisConnection.setex(checksum, expirySeconds, "")
    }
  }

  def getRedisConnection: Jedis = redisConnection

  def closeConnectionPool(): Unit = {
    redisConnection.close()
  }
}
