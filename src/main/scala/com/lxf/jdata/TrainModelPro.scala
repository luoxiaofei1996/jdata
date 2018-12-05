package com.lxf.jdata

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainModelPro {

  val arrayStr = " a1, a2, a3,brand," +
    "sex_0,sex_1,sex_2,age_d1,age_1,age_2,age_3,age_4,age_5,age_6,user_lv_cd," +
    " comment_num,  has_bad_comment,  bad_comment_rate," +
    "t1_1, t2_1, t3_1, t4_1, t5_1, t6_1,\nt1_2, t2_2, t3_2, t4_2, t5_2, t6_2,\nt1_3, t2_3, t3_3, t4_3, t5_3, t6_3,\nt1_l_3, t2_l_3, t3_l_3, t4_l_3, t5_l_3, t6_l_3,\nt1_l_5, t2_l_5, t3_l_5, t4_l_5, t5_l_5, t6_l_5,\nt1_l_7, t2_l_7, t3_l_7, t4_l_7, t5_l_7, t6_l_7,\nt1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10,\nt1_l_14, t2_l_14, t3_l_14, t4_l_14, t5_l_14, t6_l_14,\nt1_l_50, t2_l_50, t3_l_50, t4_l_50, t5_l_50, t6_l_50"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.sql

    val array = arrayStr.replace(" ", "").replace("\n", "").split(",").toArray
    val test = getDfFromFile(spark, "train_pro", array)
    val train = getDfFromFile(spark, "test", array)

    val(maxDepth , numRound , nworker )=(7,7,7)

    val paramMap = List(
      "eta" -> 0.1, //学习率
      "gamma" -> 0, //用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
      "lambda" -> 2, //控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
      "subsample" -> 1, //随机采样训练样本
      "colsample_bytree" -> 0.8, //生成树时进行的列采样
      "max_depth" -> maxDepth, //构建树的深度，越大越容易过拟合
      "min_child_weight" -> 5,
      "objective" -> "multi:softprob", //定义学习任务及相应的学习目标
      "eval_metric" -> "merror",
      "num_class" -> 21
    ).toMap


    val model: XGBoostModel = XGBoost.trainWithDataFrame(train, paramMap, numRound, nworker,
      useExternalMemory = true,
      featureCol = "features",
      labelCol = "label",
      missing = 0.0f)

    //predict the test set
    val predict: DataFrame = model.transform(test)
    predict.createTempView("predict")
    sql("select * from predict where prediction =1").show(false)

    val acc = 1.0 * sql("select * from predict where label=1 and prediction=1.0").count() / sql("select * from predict where label=1").count()

    println(s"""acc:$acc""")
  }

  def getDfFromFile(spark: SparkSession, dirName: String, features: Array[String]) = {
    import spark.sql
    spark.read.option("header", "true").parquet(dirName).createTempView(dirName)
    println(sql("select * from " + dirName + " where label =0").sample(true, 0.01, 2).count())
    val df = sql("select * from " + dirName + " where label =0")
      .sample(true, 0.01, 2)
      .union(sql("select * from " + dirName + " where label =1"))

    val arrayStr = " a1, a2, a3,brand," +
      "sex_0,sex_1,sex_2,age_d1,age_1,age_2,age_3,age_4,age_5,age_6,user_lv_cd," +
      " comment_num,  has_bad_comment,  bad_comment_rate," +
      "t1_1, t2_1, t3_1, t4_1, t5_1, t6_1,\nt1_2, t2_2, t3_2, t4_2, t5_2, t6_2,\nt1_3, t2_3, t3_3, t4_3, t5_3, t6_3,\nt1_l_3, t2_l_3, t3_l_3, t4_l_3, t5_l_3, t6_l_3,\nt1_l_5, t2_l_5, t3_l_5, t4_l_5, t5_l_5, t6_l_5,\nt1_l_7, t2_l_7, t3_l_7, t4_l_7, t5_l_7, t6_l_7,\nt1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10,\nt1_l_14, t2_l_14, t3_l_14, t4_l_14, t5_l_14, t6_l_14,\nt1_l_50, t2_l_50, t3_l_50, t4_l_50, t5_l_50, t6_l_50"


    new VectorAssembler().setInputCols(features).setOutputCol("features").transform(df)
  }


}
