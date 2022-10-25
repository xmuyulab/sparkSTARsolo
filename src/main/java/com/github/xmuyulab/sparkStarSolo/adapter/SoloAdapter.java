package com.github.xmuyulab.sparkStarSolo.adapter;

import htsjdk.samtools.util.Tuple;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SoloAdapter {

    private static volatile SoloInit soloInitInstance = null;

    private static SoloInit getSoloInit(String libPath) throws IOException {
        if (soloInitInstance == null) {
            synchronized (SoloAdapter.class) {
                if (soloInitInstance == null) {
                    System.load(libPath);
                    soloInitInstance = new SoloInit();
                }
            }
        }
        return soloInitInstance;
    }

    public static List<String> solo(String libPath, String tmpCommandLine)  {
        SoloInit soloInit;
        String[] tmpCommandArray = tmpCommandLine.split(" ");
        int n = tmpCommandArray.length;
        char[][] commandLine = new char[n][64];
        byte[][] Genome = new byte[1024][10];
        for (int i = 0; i < 1024; ++ i) {
            Genome[i] = "abcdefg".getBytes(StandardCharsets.UTF_8);
        }
        ArrayList<String> arrayList = new ArrayList<String>();
        for (int i = 0; i < n; i++) {
            commandLine[i] = (tmpCommandArray[i] + "\0").toCharArray();
        }
        try {
            soloInit = getSoloInit(libPath);
            ArrayList<String> fastqR1 = new ArrayList<String>();
            ArrayList<String> fastqR2 = new ArrayList<String>();
//            StringBuilder sbR1 = new StringBuilder(1048576);
//            StringBuilder sbR2 = new StringBuilder(1048576);
//            int i = 0;
//            for (Tuple2<String, String> temp : fastq) {
////                temp = f.split("split");
//                sbR1.append(temp._1);
//                sbR2.append(temp._2);
//                i++;
//                if (i == 1024) {
//                    i = 0;
//                    fastqR1.add(sbR1.toString());
//                    fastqR2.add(sbR2.toString());
//                    sbR1 = new StringBuilder(1048576);
//                    sbR2 = new StringBuilder(1048576);
//                }
//            }
//            if (i != 0) {
//                fastqR1.add(sbR1.toString());
//                fastqR2.add(sbR2.toString());
//            }
            String[] tmp = soloInit.runSolo(commandLine, fastqR1.toArray(new String[1]), fastqR2.toArray(new String[1]), Genome);
            arrayList = new ArrayList<String>(tmp.length);
            Collections.addAll(arrayList, tmp);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return arrayList;
    }

}
