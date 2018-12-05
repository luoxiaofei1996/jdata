package com.lxf.jdata

import ml.dmlc.xgboost4j.scala.spark.{XGBoost, XGBoostModel}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

object TrainModel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.sql
    import spark.implicits._

    spark.read.option("header","true").parquet("train").createTempView("train")
    println(sql("select * from train where label =0").sample(false, 0.01, 2).count())
    val df=sql("select * from train where label =0")
      .sample(true, 0.01, 2)
      .union(sql("select * from train where label =1"))


    val maxDepth = 7
    val numRound = 7
    val nworker = 7
    val paramMap = List(
      "eta" -> 0.1, //学习率
      "gamma" -> 0, //用于控制是否后剪枝的参数,越大越保守，一般0.1、0.2这样子。
      "lambda" -> 2, //控制模型复杂度的权重值的L2正则化项参数，参数越大，模型越不容易过拟合。
      "subsample" -> 1, //随机采样训练样本
      "colsample_bytree" -> 0.8, //生成树时进行的列采样
      "max_depth" -> maxDepth, //构建树的深度，越大越容易过拟合
      "min_child_weight" -> 5,
      "objective" -> "multi:softprob",  //定义学习任务及相应的学习目标
      "eval_metric" -> "merror",
      "num_class" -> 21
    ).toMap


    var array = "model_id,utype_1,utype_2,utype_3,utype_4,utype_5,utype_6,a1,a2,a3,brand,sex_0,sex_1,sex_2,age_d1,age_1,age_2,age_3,age_4,age_5,age_6,user_lv_cd,comment_num,has_bad_comment,bad_comment_rate".split(",")
    val df2=new VectorAssembler().setInputCols(array).setOutputCol("features").transform(df)

    df2.show(false)
    val model:XGBoostModel = XGBoost.trainWithDataFrame(df2, paramMap, numRound, nworker,
      useExternalMemory = true,
      featureCol = "features",
      labelCol = "label",
      missing = 0.0f)

    //predict the test set
    val predict:DataFrame = model.transform(df2)
    predict.createTempView("predict")
    sql("select * from predict where prediction =1").show()

    val acc=1.0*sql("select * from predict where label=1 and prediction=1.0").count()/sql("select * from predict where label=1").count()

    println(s"""acc:$acc""")
  }

}
