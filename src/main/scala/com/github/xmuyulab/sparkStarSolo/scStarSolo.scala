/*
 * @author: liuhongjin
 * @date: 2022/7/27
 */

package com.github.xmuyulab.sparkStarSolo

import com.github.xmuyulab.sparkStarSolo.adapter.SoloAdapter
import com.github.xmuyulab.sparkStarSolo.fileio.FastqInputFormat
import com.github.xmuyulab.sparkStarSolo.process.{STARProcess, WhitelistProcess}
import com.github.xmuyulab.sparkStarSolo.utils.StringUtils

import java.text.SimpleDateFormat
import java.util
import java.util.ArrayList
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}
import java.util.Date
import org.apache.hadoop.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.{Argument, CmdLineParser, Option}

import java.io.{BufferedOutputStream, BufferedWriter, File, FileWriter}
import scala.collection.JavaConversions._

object scStarSolo {

    @Option(required = true, name = "-fastqR1", usage = "fastq(cell barcode and umi)")
    val fastqR1_path: String = StringUtils.EMPTY

    @Option(required = true, name = "-fastqR2", usage = "fastq(cell barcode and umi)")
    val fastqR2_path: String = StringUtils.EMPTY

    @Option(required = false, name = "-whitelist", usage = "whitelist(soloCBwhitelist)")
    val whitelist_path: String = StringUtils.EMPTY

    @Option(required = true, name = "-index", usage = "index of STARsolo(genomeDir)")
    val index: String = StringUtils.EMPTY

    @Option(required = true, name = "-libPath", usage = "dependency")
    val libPath: String = StringUtils.EMPTY

    @Option(required = true, name = "-runThreadN", usage = "runThreadN")
    val runThreadN: String = StringUtils.EMPTY

    // 搭配Fastq文件使用, 成套的
    @Option(required = false, name = "-soloCBstart", usage = "cell barcode start base, default: 1")
    val soloCBstart: String = StringUtils.EMPTY

    @Option(required = false, name = "-soloCBlen", usage = "cell barcode length, default: 16")
    val soloCBlen: String = StringUtils.EMPTY

    @Option(required = false, name = "-soloUMIstart", usage = "UMI start base, default: 17")
    val soloUMIstart: String = StringUtils.EMPTY

    @Option(required = false, name = "-soloUMIlen", usage = "UMI length, default: 12(v3)")
    val soloUMIlen: String = StringUtils.EMPTY

    @Option(required = true, name = "-output", usage = "output path")
    val output: String = StringUtils.EMPTY

    @Argument
    val arguments: ArrayList[String] = new ArrayList[String]()

    def main(args: Array[String]): Unit = {
        // args
        val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        println(df.format(new Date()))
        val parser: CmdLineParser = new CmdLineParser(this)
        parser.setUsageWidth(300)
        val argList = new util.ArrayList[String]()
        args.foreach(arg => argList.add(arg))
        parser.parseArgument(argList)
        val conf = new SparkConf().setAppName("sparkStarSolo")
                                      .set("spark.driver.maxResultSize", "160g")
                                      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                      .set("spark.network.timeout", "1200000")
                                      .set("spark.executor.heartbeatInterval", "1000000")
                                      .set("spark.kryoserializer.buffer.max","2047mb")
                                      .set("spark.rpc.message.maxSize","2047")
                                      .set("spark.maxRemoteBlockSizeFetchToMem", "2047mb")
                                    //          .set("spark.default.parallelism", "400")

        if (conf.getOption("spark.master").isEmpty) {
            conf.setMaster("local[%d]".format(Runtime.getRuntime.availableProcessors()))
        }
        val sc = new SparkContext(conf)

        // multi thread
        val threadPool: ExecutorService = Executors.newFixedThreadPool(3)
        try {
            val future1 = new FutureTask[RDD[(Long, Text)]](new Callable[RDD[(Long, Text)]] {
                override def call(): RDD[(Long, Text)] = {
                    println("FastqR1 start:" + df.format(new Date()))
                    val tempRDD = sc.newAPIHadoopFile(fastqR1_path, classOf[FastqInputFormat], classOf[Void], classOf[Text])
                                        .map(line => line._2)
                                        .zipWithIndex()
                                        .map(line => (line._2, line._1))
                    tempRDD
                }
            })
            val future2 = new FutureTask[RDD[(Long, Text)]](new Callable[RDD[(Long, Text)]] {
                override def call(): RDD[(Long, Text)] = {
                    println("FastqR2 start:" + df.format(new Date()))
                    val tempRDD = sc.newAPIHadoopFile(fastqR2_path, classOf[FastqInputFormat], classOf[Void], classOf[Text])
                                    .map(line => line._2)
                                    .zipWithIndex()
                                    .map(line => (line._2, line._1))
                    tempRDD
                }
            })
            val future3 = new FutureTask[RDD[(String, Int)]](new Callable[RDD[(String, Int)]] {
                override def call(): RDD[(String, Int)] = {
                    WhitelistProcess.processWhitelist(sc, df, whitelist_path)
                }
            })
            threadPool.execute(future1)
            threadPool.execute(future2)
            val temp_fastqR1_RDD = future1.get()
            val temp_fastqR2_RDD = future2.get()

            val cbStart = soloCBstart.toInt - 1
            val cbEnd = soloCBstart.toInt - 1 + soloCBlen.toInt
            var fastqR2: RDD[(String, String)] = null
            if (whitelist_path.equals(StringUtils.EMPTY)) {
                fastqR2 = temp_fastqR1_RDD.join(temp_fastqR2_RDD)
                  .map(line => (line._2._1.toString, line._2._2.toString))
            } else {
                threadPool.execute(future3)
                val whitelist = future3.get()
                val fastqindex = temp_fastqR1_RDD.map(line => (line._2.toString().split("\n")(1).substring(cbStart, cbEnd), line._1))
                  .join(whitelist)
                  .map(line => (line._2._1, 1))

                fastqR2 = temp_fastqR1_RDD.join(fastqindex).map(line => (line._1, line._2._1)).join(temp_fastqR2_RDD)
                  .map(line => (line._2._1.toString, line._2._2.toString))

                fastqindex.unpersist()
            }
            fastqR2 = fastqR2.cache()

            val outputStr = output
            val indexStr = index
            val soloCBstartStr = soloCBstart
            val soloCBlenStr = soloCBlen
            val soloUMIstartStr = soloUMIstart
            val soloUMIlenStr = soloUMIlen
            val runThreadNStr = runThreadN
            val libPathStr = libPath

            val partitionNum = STARProcess.getStarPartition(sc, fastqR2.count())
            val matrix = fastqR2.repartition(partitionNum).mapPartitionsWithIndex((pIndex, it) => {
                val R1File = new File("/tmp/" + System.currentTimeMillis() + "_R1_" + pIndex + ".fastq")
                val R2File = new File("/tmp/" + System.currentTimeMillis() + "_R2_" + pIndex + ".fastq")
                val bufferedOutputR1: BufferedWriter  = new BufferedWriter(new FileWriter(R1File))
                val bufferedOutputR2: BufferedWriter  = new BufferedWriter(new FileWriter(R2File))
                it.foreach(data => {
                    bufferedOutputR1.write(data._1)
                    bufferedOutputR2.write(data._2)
                })
                bufferedOutputR1.flush()
                bufferedOutputR2.flush()
                bufferedOutputR1.close()
                bufferedOutputR2.close()
                val commandLine = STARProcess.getStarCommand(indexStr, soloCBstartStr, soloCBlenStr, soloUMIstartStr,
                    soloUMIlenStr, runThreadNStr, R1File.getAbsolutePath, R2File.getAbsolutePath)
                val result = SoloAdapter.solo(libPathStr, commandLine)
                R1File.delete()
                R2File.delete()
                result.iterator()
            }).repartition(512).map(line => {
                val tmp = line.split("_")
                val tmp_n = tmp.length
                (tmp(tmp_n - 4) + "\t" + tmp(tmp_n - 3) + "\t" + tmp(tmp_n - 2), tmp(tmp_n - 1).toInt)
            }).reduceByKey((a, b) => (a + b)).map(line => line._1 + "\t" + line._2)
            matrix.saveAsTextFile(output)

        } finally {
            threadPool.shutdown()
        }
        println(df.format(new Date()))
    }
}
