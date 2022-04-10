
package com.seu.geomesa.spark;

import java.text.SimpleDateFormat;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.SpatialRDD;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.locationtech.geomesa.utils.geohash.GeoHash;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;

import com.seu.utils.ScalaUtil;

import scala.Tuple2;

@Slf4j
public class CountByDayDemo {
    public static void main(String[] args) throws CQLException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[] {ImmutableBytesWritable.class, Result.class});
        SparkSession sparkSession = SparkSession.builder().appName("CountByDayDemo")
                .master("local[*]")
                .config(sparkConf)
                .getOrCreate();

        Map<String, String> params = new HashMap<>();
        params.put("hbase.zookeepers", "zookeeper");
        params.put("hbase.catalog", "higeo:st_gdelt");
        // HashMap转换为Scala的Map
        scala.collection.immutable.Map<String, String> scalaParams = ScalaUtil.toScalaImmutableMap(params);


        String typeName = "gdelt";

        String filter = "bbox(geom, -80, 35, -79, 36) AND dtg during 2018-01-20T00:00:00.000Z/2019-01-31T12:00:00.000Z";

        Query query = new Query(typeName, ECQL.toFilter(filter));
        SpatialRDDProvider spatialRDDProvider = GeoMesaSpark.apply(params);

        Configuration configuration = new Configuration();
        SpatialRDD rdd = spatialRDDProvider.rdd(configuration, sparkSession.sparkContext(), scalaParams, query);
        JavaRDD<SimpleFeature> simpleFeatureJavaRDD = rdd.toJavaRDD();
        JavaPairRDD<String, Integer> pairRDD = simpleFeatureJavaRDD
            .mapPartitionsToPair((PairFlatMapFunction<Iterator<SimpleFeature>, String, Integer>) partition -> {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                FilterFactory2 filterFactory2 = CommonFactoryFinder.getFilterFactory2();
                PropertyName dtg = filterFactory2.property("dtg");
                while (partition.hasNext()) {
                    SimpleFeature simpleFeature = partition.next();
                    Tuple2<String, Integer> tuple = new Tuple2<>(sdf.format(dtg.evaluate(simpleFeature)), 1);
                    list.add(tuple);
                }
                return list.iterator();
            })
            .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        pairRDD.collect().forEach(val -> System.out.println(val._1() + "\t" + val._2()));


    }
}
