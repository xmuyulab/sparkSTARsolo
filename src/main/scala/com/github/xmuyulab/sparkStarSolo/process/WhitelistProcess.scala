package com.github.xmuyulab.sparkStarSolo.process

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Date

object WhitelistProcess {

  def processWhitelist(sc: SparkContext, df: SimpleDateFormat, whitelistPath: String): RDD[(String, Int)] = {

    println("whitelist start:" + df.format(new Date()))
    val tempArray = sc.textFile(whitelistPath).repartition(512).flatMap(line => {
      val n = line.length
      var sb = new StringBuilder()
      var res = new StringBuilder()
      var i = 0
      sb.append(line)
      res.append(line)
      for (i <- 0 until n) {
        if (sb.charAt(i) == 'A') {
          sb.setCharAt(i, 'T')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'G')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'C')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'A')
        } else if (sb.charAt(i) == 'T') {
          sb.setCharAt(i, 'G')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'C')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'A')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'T')
        } else if (sb.charAt(i) == 'G') {
          sb.setCharAt(i, 'C')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'A')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'T')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'G')
        } else {
          sb.setCharAt(i, 'A')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'T')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'G')
          res.append("\t" + sb.toString)
          sb.setCharAt(i, 'C')
        }
      }
      res.toString.split("\t")
    }).map(line => (line,1)).reduceByKey((a,b) => a+b)
    println("whitelist end：" + df.format(new Date()))
    tempArray

  }

}
