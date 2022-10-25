package com.github.xmuyulab.sparkStarSolo.utils

import scala.collection.mutable.ArrayBuffer

object CommandUtils {
    // ./STAR --genomeDir /home/liuyu/solo/index --readFilesIn /home/liuyu/dataset/1/fastq/R2.fastq /home/liuyu/dataset/1/fastq/R1.fastq --soloType CB_UMI_Simple --soloCBwhitelist None --soloCBstart 1 --soloCBlen 16 --soloUMIstart 17 --soloUMIlen 12
    def makeSTARCommand(genomeDir: String, runThreadN: String, soloType: String, soloCBwhitelist: String, soloCBstart: String, soloCBlen: String, soloUMIstart: String, soloUMIlen: String): Array[Array[Char]] = {
        var commandLine = Array.ofDim[Char](17, 32)
        commandLine(0) = "STAR\0".toCharArray
        commandLine(1) = "genomeDir\0".toCharArray
        commandLine(2) = (genomeDir + "\0").toCharArray
        commandLine(3) = "runThreadN\0".toCharArray
        commandLine(4) = (runThreadN + "\0").toCharArray
        commandLine(5) = "soloType\0".toCharArray
        commandLine(6) = (soloType + "\0").toCharArray
        commandLine(7) = "soloCBwhitelist\0".toCharArray
        commandLine(8) = (soloCBwhitelist + "\0").toCharArray
        commandLine(9) = "soloCBstart\0".toCharArray
        commandLine(10) = (soloCBstart + "\0").toCharArray
        commandLine(11) = "soloCBlen\0".toCharArray
        commandLine(12) = (soloCBlen + "\0").toCharArray
        commandLine(13) = "soloUMIstart\0".toCharArray
        commandLine(14) = (soloUMIstart + "\0").toCharArray
        commandLine(15) = "soloUMIlen\0".toCharArray
        commandLine(16) = (soloUMIlen + "\0").toCharArray
        commandLine
    }
}