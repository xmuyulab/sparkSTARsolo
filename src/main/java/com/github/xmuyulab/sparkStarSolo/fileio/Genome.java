package com.github.xmuyulab.sparkStarSolo.fileio;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

public class Genome {
    public byte[][] getGenome(String path) {
        List<byte[]> Genome = new LinkedList<byte[]>();
        try {
            FileInputStream tempIn = new FileInputStream(path);
            byte[] tempByte = new byte[67108864];
            int i = 0;
            int len = tempIn.read(tempByte);
            while (len != -1) {
                byte[] temp = Arrays.copyOfRange(tempByte, 0, len);
                Genome.add(temp);
                len = tempIn.read(tempByte);
            }
        } catch (IOException e) {

        }
        return (byte[][])Genome.toArray(new byte[Genome.size()][]);
    }
    
}
