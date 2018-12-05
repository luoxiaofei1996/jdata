package com.lxf.jdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CheckData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.read.option("header", "true").parquet("train-pro")
    df.createTempView("train")
    import spark.sql
    import spark.implicits._

    for (elem <- df.columns) {
      sql("select * from train where "+elem+" is null").show()
    }


    sql("select count(1) from train where label=1").show()
    sql("select count(1) from train where label=0").show()
    sql("select sum(label)/count(*) from train ").show()
    sql("select * from train").show()
  }

}
