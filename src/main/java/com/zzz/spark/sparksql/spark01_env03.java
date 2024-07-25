package com.zzz.spark.sparksql;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class spark01_env03 {
    public static void main(String[] args) {

        // 更常用的方法是先创建一个SparkConf   在传进去
        SparkConf conf = new SparkConf().setAppName("sql").setMaster("local[*]");
        final SparkSession sparkSession = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        // TODO 环境的转换
        // SparkSession -> SparkContext
        // SparkContext sc = sparkSession.sparkContext();

        // SparkContext -> SparkSession
        // new SparkSession(new SparkContext(conf));

        // TODO java版本的SparkContext
        // SparkContext sparkContext = sparkSession.sparkContext();
        // JavaSparkContext jsc = new JavaSparkContext(sparkContext);



        sparkSession.close();

    }
}
