package com.zzz.spark.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class spark01_model {
    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("sparkSQL")
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().json("data/user.json");

        ds.createOrReplaceTempView("user");

        String sql = "select age+1 from user";
        Dataset<Row> sqlDS = sparkSession.sql(sql);

        sqlDS.show();

        sparkSession.close();

    }
}
