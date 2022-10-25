package com.github.xmuyulab.sparkStarSolo.jstar;

import com.github.xmuyulab.sparkStarSolo.jstar.HeadFile;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

public class test {

    static {        
        System.load("/home/liuyu/STAR/source/libSolo.so");
    }

    public static void main(String[] args) {
        HeadFile h = new HeadFile();
        String tmpCommandLine = "STAR --genomeDir /home/liuyu/solo/index --readFilesIn /home/liuyu/dataset/1/fastq/R2.fastq /home/liuyu/dataset/1/fastq/R1.fastq --soloType CB_UMI_Simple --soloCBwhitelist None --soloCBstart 1 --soloCBlen 16 --soloUMIstart 17 --soloUMIlen 12 --runThreadN 32";
        String[] tmpCommandArray = tmpCommandLine.split(" ");
        char[][] commandLine = new char[20][64];
        System.out.println(tmpCommandArray.length);
        for (int i = 0; i < 20; i++) {
            commandLine[i] = (tmpCommandArray[i] + "\0").toCharArray();
        }
        char[][] fastqR1 = new char[][] {
            {'f', 'a', 's', 't', 'q', 'R', '1', '\0'},
            {'l', 'i', 'n', 'e', '\0', '\0', '\0', '\0'}
        };
        char[][] fastqR2 = new char[][] {
            {'f', 'a', 's', 't', 'q', 'R', '2', '\0'},
            {'l', 'i', 'n', 'e', '\0', '\0', '\0', '\0'}
        };
        List<byte[]> Genome = new LinkedList<byte[]>();
        try {
            FileInputStream tempIn = new FileInputStream("/home/liuyu/solo/index/Genome");
            byte[] tempByte = new byte[67108864];
            int i = 0;
            int len = tempIn.read(tempByte);
            while (len != -1) {
                byte[] temp = Arrays.copyOfRange(tempByte, 0, len);
                Genome.add(temp);
                len = tempIn.read(tempByte);
            }
            char[][] cL = h.solo(commandLine, fastqR1, fastqR2, (byte[][])Genome.toArray(new byte[Genome.size()][]));
            for (i = 0; i < cL.length; i++) {
                for (int j = 0; j < cL[i].length; j++) {
                    System.out.println(cL[i][j]);
                }
            }
        } catch (IOException e) {

        }
    }
}
