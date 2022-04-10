package com.seu.geomesa.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class GdeltDataDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {ImmutableBytesWritable.class, Result.class});
        SparkSession sparkSession = SparkSession.builder().appName("GdeltDataDemo")
                .config(sparkConf)
                .getOrCreate();

        Map<String, String> params = new HashMap<>();
        params.put("hbase.zookeepers", "zookeeper");
        params.put("hbase.catalog", "higeo:st_gdelt");
        log.info("params: {}", params);

        Dataset<Row> gdeltDF = sparkSession.read()
                .format("geomesa")
                .options(params)
                .option("geomesa.feature", "gdelt")
                .load();

        gdeltDF.createOrReplaceTempView("tmp_gdelt");
        String querySql = "select * from tmp_gdelt where st_contains(st_makeBBox(0,0,180,90), geom)";
        sparkSession.sql(querySql).show(10);
    }
}
