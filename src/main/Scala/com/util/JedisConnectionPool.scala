package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)//最大的连接数

  config.setMaxIdle(10)//每次最大获取10个

  private val pool = new JedisPool(config,"hadoop0003",6379,10000)

  def getConnection():Jedis={
    pool.getResource
  }

}
