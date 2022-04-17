package com.seu.geomesa.spark.core;

import com.seu.utils.ScalaUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.spark.GeoMesaSpark;
import org.locationtech.geomesa.spark.SpatialRDD;
import org.locationtech.geomesa.spark.SpatialRDDProvider;
import org.opengis.filter.Filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CoreDemo {
    public static void main(String[] args) throws CQLException {
        SparkSession spark = SparkSession.builder().appName("GeoMesa Spark Core Demo").master("local[*]").getOrCreate();

        Map<String, String> params = new HashMap<>();
        params.put("hbasehbase.zookeepers", "zookeeper");
        params.put("hbase.catalog", "higeo:st_gdelt");
        scala.collection.immutable.Map<String, String> map = ScalaUtil.toScalaImmutableMap(params);

        SpatialRDDProvider spatialRDDProvider = GeoMesaSpark.apply(params);
        Filter filter = ECQL.toFilter("CONTAINS(POLYGON((0 0, 0 90, 90 90, 90 0, 0 0)), geom)");
        Query query = new Query("gdelt", filter);

        SpatialRDD spatialRDD = spatialRDDProvider.rdd(new Configuration(), spark.sparkContext(), map, query);
        System.out.println(spatialRDD.count());
    }
}
