package com.github.xmuyulab.sparkStarSolo.adapter;

public class SoloInit {

    public SoloInit() {
        
    }

    public native String[] runSolo(char[][] commandLine, String[] fastqR1, String[] fastqR2, byte[][] Genome);

}
