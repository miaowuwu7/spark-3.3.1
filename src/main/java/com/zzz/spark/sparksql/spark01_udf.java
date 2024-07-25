package com.zzz.spark.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType$;

public class spark01_udf {
    public static void main(String[] args) {

        final SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("sparkSQL")
                .getOrCreate();

        Dataset<Row> ds = sparkSession.read().json("data/user.json");

        ds.createOrReplaceTempView("user");

        // 用户自定义函数.udf   在sql语言使用
        // register方法的第一个参数为自定义的方法名
        //               第二个参数为计算逻辑  IN => OUT
        //               第三个参数为返回的数据类型（貌似是SQL中的类型）
        sparkSession.udf().register("prefixName", new UDF1<String, String>() {
            public String call(String name) throws Exception {
                return "Name:" + name;
            }
//        }, StringType$.MODULE$);    或者下面这个
        }, DataTypes.StringType);

        String sql = ("select age,prefixName(username) from user");

        Dataset<Row> sqlDS = sparkSession.sql(sql);
        sqlDS.show();

        sparkSession.close();

    }
}
