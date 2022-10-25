package com.github.xmuyulab.sparkStarSolo.process

import com.redislabs.provider.redis._

import java.util.List
import java.util.LinkedList
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import scala.collection.JavaConversions._
import scala.collection.mutable.HashSet

object SetRDDFromRedisSpark {
    def set(sc: SparkContext, indexName: String): Unit = {
        // Text File => RDD[String]
        val chrLength = sc.fromRedisKV(indexName + "_chrLength*", 1).map(line => line._2)
        val B_chrLength = sc.broadcast(chrLength.collect())
        chrLength.unpersist()
        val chrName = sc.fromRedisKV(indexName + "_chrName*", 1).map(line => line._2)
        val B_chrName = sc.broadcast(chrName.collect())
        chrName.unpersist()
        val chrNameLength = sc.fromRedisKV(indexName + "_chrNameLength*", 1).map(line => line._2)
        val B_chrNameLength = sc.broadcast(chrNameLength.collect())
        chrNameLength.unpersist()
        val chrStart = sc.fromRedisKV(indexName + "_chrStart*", 1).map(line => line._2)
        val B_chrStart = sc.broadcast(chrStart.collect())
        chrStart.unpersist()
        val exonGeTrInfo = sc.fromRedisKV(indexName + "_exonGeTrInfo*", 1).map(line => line._2)
        val B_exonGeTrInfo = sc.broadcast(exonGeTrInfo.collect())
        exonGeTrInfo.unpersist()
        val exonInfo = sc.fromRedisKV(indexName + "_exonInfo*", 1).map(line => line._2)
        val B_exonInfo = sc.broadcast(exonInfo.collect())
        exonInfo.unpersist()
        val geneInfo = sc.fromRedisKV(indexName + "_geneInfo*", 1).map(line => line._2)
        val B_geneInfo = sc.broadcast(geneInfo.collect())
        geneInfo.unpersist()
        val genomeParameters = sc.fromRedisKV(indexName + "_geneParameters*", 1).map(line => line._2)
        val B_genomeParameters = sc.broadcast(genomeParameters.collect())
        genomeParameters.unpersist()
        val Logout = sc.fromRedisKV(indexName + "_Log*", 1).map(line => line._2)
        val B_Logout = sc.broadcast(Logout.collect())
        Logout.unpersist()
        val sjdbInfo = sc.fromRedisKV(indexName + "_sjdbInfo*").map(line => line._2)
        val B_sjdbInfo = sc.broadcast(sjdbInfo.collect())
        sjdbInfo.unpersist()
        val sjdbListFromGTF = sc.fromRedisKV(indexName + "_sjdbListFromGTF*").map(line => line._2)
        val B_sjdbListFromGTF = sc.broadcast(sjdbListFromGTF.collect())
        sjdbListFromGTF.unpersist()
        val sjdbListOut = sc.fromRedisKV(indexName + "_sjdbListOut*").map(line => line._2)
        val B_sjdbListOut = sc.broadcast(sjdbListOut.collect())
        sjdbListOut.unpersist()
        val transcriptInfo = sc.fromRedisKV(indexName + "_transcriptInfo*").map(line => line._2)
        val B_transcriptInfo = sc.broadcast(transcriptInfo.collect())
        transcriptInfo.unpersist()
        // val B_chrLength = sc.broadcast(chrLength.collect())
        // val B_chrName = sc.broadcast(chrName)
        // val B_chrNameLength = sc.broadcast(chrNameLength)
        // val B_chrStart = sc.broadcast(chrStart)
        // val B_exonGeTrInfo = sc.broadcast(exonGeTrInfo)
        // val B_exonInfo = sc.broadcast(exonInfo)
        // val B_geneInfo = sc.broadcast(geneInfo)
        // val B_genomeParameters = sc.broadcast(genomeParameters)
        // val B_Logout = sc.broadcast(Logout)
        // val B_sjdbInfo = sc.broadcast(sjdbInfo)
        // val B_sjdbListFromGTF = sc.broadcast(sjdbListFromGTF)
        // val B_sjdbListOut = sc.broadcast(sjdbListOut)
        // val B_transcriptInfo = sc.broadcast(transcriptInfo)
        // Binary File
        val Genome = sc.fromRedisHash(indexName + "_Genome").map(
            line => (line._1.toInt, line._2)
        ).sortByKey().map(line => line._2).collect()
        val B_Genome = sc.broadcast(Genome)
        // val B_Genome = sc.broadcast(Genome)
    }
}