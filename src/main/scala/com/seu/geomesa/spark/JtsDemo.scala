package com.seu.geomesa.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

object JtsDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JtsDemo").master("local[*]").getOrCreate().withJTS


    val schema = StructType(Array(
      StructField("name", StringType, nullable = false),
      StructField("pointText", StringType, nullable = false),
      StructField("polygonText", StringType, nullable = false),
      StructField("latitude", DoubleType, nullable = false),
      StructField("longitude", DoubleType, nullable = false)))

    val dataFile = this.getClass.getClassLoader.getResource("data/jts-example.csv").getPath
    val df = spark.read
      .schema(schema)
      .option("sep", "-")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(dataFile)
    df.show()

    df.createOrReplaceTempView("df")
    val altered_df = spark.sql("SELECT *, st_polygonFromText(polygonText) as polygon, st_makePoint(longitude, latitude) as point from df")
    altered_df.show()

    val polygon = spark.sql("SELECT st_polygonFromText('POLYGON((-71.1776585052917 42.3902909739571,-71.1776820268866 42.3903701743239,-71.1776063012595 42.3903825660754,-71.1775826583081 42.3903033653531,-71.1776585052917 42.3902909739571))') as polygon")
    polygon.show()

    val point = new GeometryFactory().createPoint(new Coordinate(3.4, 5.6))
    println(point)

    df.where(st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), expr("st_makePoint(longitude, latitude)")))
      .show()
    altered_df.where(st_contains(st_makeBBOX(0.0, 0.0, 90.0, 90.0), col("point")))
      .show()
  }
}

