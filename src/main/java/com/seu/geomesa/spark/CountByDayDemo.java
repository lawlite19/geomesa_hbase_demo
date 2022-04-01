package com.seu.geomesa.spark;

import org.apache.hadoop.conf.Configuration;
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
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import scala.Tuple2;
import scala.collection.JavaConversions;

import java.text.SimpleDateFormat;
import java.util.*;

public class CountByDayDemo {
    public static void main(String[] args) throws CQLException {
        SparkSession sparkSession = SparkSession.builder().appName("CountByDayDemo")
                .getOrCreate();

        Map<String, String> params = new HashMap<>();
        params.put("zookeepers", "zookeeper");
        params.put("tableName", "higeo:st_gdelt");

        scala.collection.immutable.Map scalaMap = (scala.collection.immutable.Map) JavaConversions.mapAsScalaMap(params);

        String typeName = "higeo:st_gdelt";

        String filter = "bbox(geom, -80, 35, -79, 36) AND dtg during 2014-01-01T00:00:00.000Z/2014-01-31T12:00:00.000Z";

        Query query = new Query(typeName, ECQL.toFilter(filter));
        SpatialRDDProvider spatialRDDProvider = GeoMesaSpark.apply(params);

        Configuration configuration = new Configuration();
        SpatialRDD rdd = spatialRDDProvider.rdd(configuration, sparkSession.sparkContext(), scalaMap, query);
        JavaRDD<SimpleFeature> simpleFeatureJavaRDD = rdd.toJavaRDD();
        JavaPairRDD<String, Integer> pairRDD = simpleFeatureJavaRDD.mapPartitionsToPair((PairFlatMapFunction<Iterator<SimpleFeature>, String, Integer>) partition -> {
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
        }).reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        pairRDD.collect().forEach(val -> System.out.println(val._1() + "\t" + val._2()));

    }
}
