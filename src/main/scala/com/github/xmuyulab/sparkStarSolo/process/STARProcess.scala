package com.github.xmuyulab.sparkStarSolo.process

import org.apache.spark.SparkContext

object STARProcess {

  def getStarCommand(index: String, soloCBstart: String, soloCBlen: String, soloUMIstart: String, soloUMIlen: String,
                     runThreadN: String, R1String: String, R2String: String): String = {

    val commandLine = "STAR --genomeDir " + index + " --readFilesIn " + R2String + " " + R1String + " --soloType CB_UMI_Simple --soloCBwhitelist None "  +
      "--soloCBstart " +  soloCBstart  + " --soloCBlen " + soloCBlen + " --soloUMIstart " + soloUMIstart + " --soloUMIlen "  +
      soloUMIlen +  " --runThreadN " + runThreadN
    commandLine

  }

  def getStarPartition(sc: SparkContext, count: Long): Int = {
    val partitionCount = 1000000

    if (sc.isLocal || count < partitionCount) return 1
    var partition = math.min(sc.statusTracker.getExecutorInfos.length - 1, count / partitionCount)
    if (partition < 1)  partition = 1
    partition.toInt
  }

}
