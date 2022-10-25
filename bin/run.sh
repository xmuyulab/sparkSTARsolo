spark_master=spark://10.24.80.103:7077
executor_memory=64G
executor_cores=16
driver_memory=32G
total_executor_cores=64

/usr/local/service/spark/bin/spark-submit --class com.github.xmuyulab.sparkStarSolo.scStarSolo \
    --master ${spark_master} \
    --executor-memory ${executor_memory} \
    --driver-memory ${driver_memory} \
    --executor-cores ${executor_cores} \
    --total-executor-cores ${total_executor_cores} \
    /data/sparkStarSolo-1.0-SNAPSHOT-jar-with-dependencies.jar  \
    -fastqR1 hdfs:///data/pbmc_human/pbmc_10k_v3_1.fastq \
    -fastqR2 hdfs:///data/pbmc_human/pbmc_10k_v3_2.fastq  \
    -whitelist file:///data/3M-february-2018.txt \
    -index /data/HumanIndex \
    -libPath /data/libSolo.so \
    -runThreadN 16 \
    -soloCBstart 1 \
    -soloCBlen 16 \
    -soloUMIstart 17 \
    -soloUMIlen 12 \
    -output hdfs:///data/pbmc_human/64cores \

