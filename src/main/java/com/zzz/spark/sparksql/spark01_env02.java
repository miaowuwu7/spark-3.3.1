package com.zzz.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class spark01_env02 {
    public static void main(String[] args) {

        // todo 构建环境对象
        // 一般采用构建器模式，不采用直接new
        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("sparkSQL")
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().json("data/user.json");

        ds.createOrReplaceTempView("user");

        String sql = "select age from user";
        Dataset<Row> sqlDS = sparkSession.sql(sql);

        sqlDS.show();

        sparkSession.close();

    }
}
