package com.seu.geomesa.scala.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.geotools.data.Query
import org.geotools.feature.simple.{SimpleFeatureImpl, SimpleFeatureTypeImpl}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.spark.SpatialRDD.toGeoJSONString
import org.locationtech.geomesa.spark.{GeoMesaSpark, GeoMesaSparkKryoRegistrator}

import java.util

object CoreDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.registerKryoClasses(Array(classOf[ImmutableBytesWritable]))
    val spark = SparkSession.builder().appName("JtsDemo").master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config(conf)
      .getOrCreate()
    // DataStore params to a hypothetical GeoMesa Accumulo table
    val dsParams = Map(
      "hbase.zookeepers" -> "zookeeper",
      "hbase.catalog" -> "higeo:st_gdelt")
    val map = new util.HashMap[String, String]()
    dsParams.foreach(kv => map.put(kv._1, kv._2))


    // create RDD with a geospatial query using GeoMesa functions
    val spatialRDDProvider = GeoMesaSpark(map)
    val filter = ECQL.toFilter("CONTAINS(POLYGON((0 0, 0 90, 90 90, 90 0, 0 0)), geom)")
    val query = new Query("gdelt", filter)
    val resultRDD = spatialRDDProvider.rdd(new Configuration, spark.sparkContext, dsParams, query)

    println(resultRDD.count())
    resultRDD.take(10).foreach(println)
  }
}
