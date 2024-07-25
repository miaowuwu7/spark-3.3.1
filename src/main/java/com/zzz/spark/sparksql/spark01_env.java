package com.zzz.spark.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class spark01_env {
    public static void main(String[] args) {

        // todo 构建环境对象
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("SparkSQL");
        SparkContext sc = new SparkContext(conf);

        SparkSession sparkSession = new SparkSession(sc);


    }
}
