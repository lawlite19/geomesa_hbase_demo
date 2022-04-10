package com.seu.geomesa.spark.jts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.geomesa.spark.jts.package$;


import java.util.ArrayList;
import java.util.List;

public class JtsDemo {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JtsDemo").master("local[*]").getOrCreate();
        // 使用geomesa spark jts, scala中使用的隐式类，java中调用如下
        package$.MODULE$.initJTS(spark.sqlContext());

        String dataFile = JtsDemo.class.getResource("/data/jts-example.csv").getPath();
        System.out.println("dataFile: " + dataFile);

        // 创建schema
        List<StructField> schemaFields = new ArrayList<>();
        schemaFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("pointText", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("polygonText", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("latitude", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("longitude", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(schemaFields);

        // 读取数据
        Dataset<Row> df = spark.read()
                .schema(schema)
                .option("sep", "-")
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv(dataFile);
        df.show();

        df.createOrReplaceTempView("df");
        Dataset<Row> altered_df = spark.sql("SELECT *, st_polygonFromText(polygonText) as polygon, st_makePoint(longitude, latitude) as point from df");
        altered_df.show();
    }
}
