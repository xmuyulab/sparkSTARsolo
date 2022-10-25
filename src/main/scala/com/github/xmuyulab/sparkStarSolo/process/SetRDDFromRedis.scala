package com.github.xmuyulab.sparkStarSolo.process

import com.redislabs.provider.redis._

import java.util.List
import java.util.LinkedList
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster

import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

object SetRDDFromRedis {
    def set(sc: SparkContext, indexName: String): LinkedList[java.util.List[String]] = {
        val hostAndPort = new HashSet[HostAndPort]()
        val hostAndPort1 = new HostAndPort("10.24.80.101", 7000)
        val hostAndPort2 = new HostAndPort("10.24.80.102", 7000)
        val hostAndPort3 = new HostAndPort("10.24.80.102", 7001)
        val hostAndPort4 = new HostAndPort("10.24.80.103", 7000)
        val hostAndPort5 = new HostAndPort("10.24.80.105", 7000)
        val hostAndPort6 = new HostAndPort("10.24.80.105", 7001)
        hostAndPort.add(hostAndPort1)
        hostAndPort.add(hostAndPort2)
        hostAndPort.add(hostAndPort3)
        hostAndPort.add(hostAndPort4)
        hostAndPort.add(hostAndPort5)
        hostAndPort.add(hostAndPort6)
        val jedis = new JedisCluster(hostAndPort)
        val indexList = new LinkedList[List[String]]()
        val textList = new LinkedList[String]()
        textList.add(jedis.get(indexName + "_chrLength"))
        textList.add(jedis.get(indexName + "_chrName"))
        textList.add(jedis.get(indexName + "_chrNameLength"))
        textList.add(jedis.get(indexName + "_chrStart"))
        textList.add(jedis.get(indexName + "_exonGeTrInfo"))
        textList.add(jedis.get(indexName + "_exonInfo"))
        textList.add(jedis.get(indexName + "_geneInfo"))
        textList.add(jedis.get(indexName + "_geneParameters"))
        textList.add(jedis.get(indexName + "_Log"))
        textList.add(jedis.get(indexName + "_sjdbInfo")) 
        textList.add(jedis.get(indexName + "_sjdbListFromGTF"))
        textList.add(jedis.get(indexName + "_sjdbListOut"))
        textList.add(jedis.get(indexName + "_transcriptInfo"))
        indexList.add(textList)
        var genomeList = jedis.lrange(indexName + "_Genome", 0, -1)
        var SAList = jedis.lrange(indexName + "_SA", 0, 9)
        var i = 10
        var SALen = jedis.llen(indexName + "_SA").toInt
        while (i < SALen) {
            if ((i + 9) < SALen) {
                var temp = jedis.lrange(indexName + "_SA", i, i + 9)
                println(i)
                SAList = SAList ++: temp
            } else {
                SAList = SAList ++: jedis.lrange(indexName + "_SA", i, -1).toList
            }
            i = i + 10
        }
        
        var SAindexList = jedis.lrange(indexName + "_SAindex", 0, -1)
        indexList.add(genomeList)
        indexList.add(SAList)
        indexList.add(SAindexList)
        jedis.close()
        return indexList
    }
}

// Text File => RDD[String]
        // val chrLength = sc.fromRedisKV(indexName + "_chrLength*", 1).map(line => line._2).collect().toList
        // val chrName = sc.fromRedisKV(indexName + "_chrName*", 1).map(line => line._2).collect().toList
        // val chrNameLength = sc.fromRedisKV(indexName + "_chrNameLength*", 1).map(line => line._2).collect().toList
        // val chrStart = sc.fromRedisKV(indexName + "_chrStart*", 1).map(line => line._2).collect().toList
        // val exonGeTrInfo = sc.fromRedisKV(indexName + "_exonGeTrInfo*", 1).map(line => line._2).collect().toList
        // val exonInfo = sc.fromRedisKV(indexName + "_exonInfo*", 1).map(line => line._2).collect().toList
        // val geneInfo = sc.fromRedisKV(indexName + "_geneInfo*", 1).map(line => line._2).collect().toList
        // val genomeParameters = sc.fromRedisKV(indexName + "_geneParameters*", 1).map(line => line._2).collect().toList
        // val Logout = sc.fromRedisKV(indexName + "_Log*", 1).map(line => line._2).collect().toList
        // val sjdbInfo = sc.fromRedisKV(indexName + "_sjdbInfo*").map(line => line._2).collect().toList
        // val sjdbListFromGTF = sc.fromRedisKV(indexName + "_sjdbListFromGTF*").map(line => line._2).collect().toList
        // val sjdbListOut = sc.fromRedisKV(indexName + "_sjdbListOut*").map(line => line._2).collect().toList
        // val transcriptInfo = sc.fromRedisKV(indexName + "_transcriptInfo*").map(line => line._2).collect().toList
        // // Binary File
        // val Genome = new LinkedList<String>()
        // sc.fromRedisList(indexName + "_Genome", 160).foreach(record => {Genome.add(record)})
        // // val SA = sc.fromRedisList(indexName + "_SA", 160).collect().toList
        // // val SAindex = sc.fromRedisList(indexName + "_SAindex", 160).collect().toList
        // println(Genome.getClass.getSimpleName)
        // val indexRDD = chrLength ++: chrName ++: chrNameLength ++: chrNameLength ++: chrStart ++: exonGeTrInfo ++: exonInfo ++: geneInfo ++: genomeParameters ++: Logout ++: sjdbInfo ++: sjdbListFromGTF ++: sjdbListOut ++: transcriptInfo