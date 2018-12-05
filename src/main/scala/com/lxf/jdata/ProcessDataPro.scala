package com.lxf.jdata

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProcessDataPro {

  case class User(userId: Int, age: Int, sex_0: Int, sex_1: Int, sex_2: Int,
                  age_d1: Int, age_1: Int, age_2: Int, age_3: Int, age_4: Int, age_5: Int, age_6: Int,
                  user_lv_cd: Int, user_reg_tm: String)

  case class Product(productId: Int, a1: Int, a2: Int, a3: Int, brand: Int)

  case class Label(userId: Int, productId: Int, label: Int)

  //cate无意义，删去
  case class Comment(dt: String, productId: Int, comment_num: Int, has_bad_comment: Int, bad_comment_rate: Double)

  case class UserAction(userId: Int, productId: Int, time: String, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction(userId: Int, productId: Int, day: Int, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction2(userId: Int, productId: Int, day: Int, model_id: Int,
                            utype_1: Int, utype_2: Int, utype_3: Int, utype_4: Int, utype_5: Int, utype_6: Int,
                            cate: Int, brand: Int)

  case class ProUserAction3(userId: Int, productId: Int,

                            t1_1: Int, t2_1: Int, t3_1: Int, t4_1: Int, t5_1: Int, t6_1: Int, //第一天
                            t1_2: Int, t2_2: Int, t3_2: Int, t4_2: Int, t5_2: Int, t6_2: Int,
                            t1_3: Int, t2_3: Int, t3_3: Int, t4_3: Int, t5_3: Int, t6_3: Int,
                            t1_l_3: Int, t2_l_3: Int, t3_l_3: Int, t4_l_3: Int, t5_l_3: Int, t6_l_3: Int, //前三天
                            t1_l_5: Int, t2_l_5: Int, t3_l_5: Int, t4_l_5: Int, t5_l_5: Int, t6_l_5: Int,
                            t1_l_7: Int, t2_l_7: Int, t3_l_7: Int, t4_l_7: Int, t5_l_7: Int, t6_l_7: Int,
                            t1_l_10: Int, t2_l_10: Int, t3_l_10: Int, t4_l_10: Int, t5_l_10: Int, t6_l_10: Int,
                            t1_l_14: Int, t2_l_14: Int, t3_l_14: Int, t4_l_14: Int, t5_l_14: Int, t6_l_14: Int,
                            t1_l_50: Int, t2_l_50: Int, t3_l_50: Int, t4_l_50: Int, t5_l_50: Int, t6_l_50: Int
                           )

  case class Train(userId: Int, productId: Int, model_id: Int, utype_1: Int, utype_2: Int, utype_3: Int, utype_4: Int, utype_5: Int, utype_6: Int, cate: Int, brand: Int, label: Int)

  def main(args: Array[String]): Unit = {
    val end = 101
    val (f_start, f_end, l_start, l_end) = (end - 50, end, end + 1, end + 5)
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import spark.sql

    sql("use jdata")

    //    //用户表
    sql("select * from user where age !='NULL'")
      .map { x =>
        val age = x.getString(1)
        var pro_age = 0
        if (age != null) {
          pro_age = age.substring(0, 2).toInt
        } else {
          pro_age = -2
        }
        pro_age = pro_age match {
          case 15 => 1
          case 16 => 2
          case 26 => 3
          case 36 => 4
          case 46 => 5
          case 56 => 6
          case -1 => -1
          case _ => -2
        }
        var (age_d1, age_1, age_2, age_3, age_4, age_5, age_6) = (0, 0, 0, 0, 0, 0, 0)
        pro_age match {
          case 1 => age_1 = 1
          case 2 => age_2 = 1
          case 3 => age_3 = 1
          case 4 => age_4 = 1
          case 5 => age_5 = 1
          case 6 => age_6 = 1
          case -1 => age_d1 = 1
          case _ =>
        }
        var (sex_0, sex_1, sex_2) = (0, 0, 0)
        x.getString(2).toInt match {
          case 0 => sex_0 = 1
          case 1 => sex_1 = 1
          case 2 => sex_2 = 1
          case _ => sex_2 = 1
        }
        User(x.getString(0).toInt, pro_age,
          sex_0, sex_1, sex_2,
          age_d1, age_1, age_2, age_3, age_4, age_5, age_6,
          x.getString(3).toInt, x.getString(4))
      }
      .createTempView("pro_user")
    //
    //    //商品表
    sql("select * from product")
      .map(x => Product(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt, x.getString(5).toInt))
      .createTempView("pro_product")

    //    //评论表
    sql("select dt, sku_id, comment_num, has_bad_comment, bad_comment_rate from comment")
      .map(x => Comment(x.getString(0), x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt, x.getString(4).toDouble))
      .createTempView("temp_comment")

    sql("select productId, sum(comment_num) comment_num, sum(has_bad_comment) has_bad_comment, avg(bad_comment_rate) bad_comment_rate from temp_comment group by productId")
      .createTempView("pro_comment")
    //用户行为表
    sql("select * from user_action where cate=8")
      .map {
        x => {
          var modelId: Int = -2 //没有点击行为的设为-2
          if (x.getString(3) != "") {
            modelId = x.getString(3).toInt
          }
          val time = x.getString(2)
          val date = time.split(" ")(0).split("-")
          val day: Int = date(1) match {
            case "01" => date(2).toInt
            case "02" => 31 + date(2).toInt
            case "03" => 31 + 29 + date(2).toInt
            case "04" => 31 + 29 + 31 + date(2).toInt //要预测的日期是 31+19+31+16 - 31+19+31+20 之间
          }
          ProUserAction(x.getString(0).toDouble.toInt, x.getString(1).toInt, day, modelId, x.getString(4).toInt, x.getString(5).toInt, x.getString(6).toInt)
        }
      }
      .createTempView("pro_action")


    //开始抽取特征
    sql(s"""select * from pro_action where day >= $f_start and day<=$f_end""")
      .map(x => {
        var (utype_1, utype_2, utype_3, utype_4, utype_5, utype_6) = (0, 0, 0, 0, 0, 0)
        x.getInt(4) match {
          case 1 => utype_1 = 1
          case 2 => utype_2 = 1
          case 3 => utype_3 = 1
          case 4 => utype_4 = 1
          case 5 => utype_5 = 1
          case 6 => utype_6 = 1
        }
        ProUserAction2(x.getInt(0), x.getInt(1), x.getInt(2), x.getInt(3),
          utype_1, utype_2, utype_3, utype_4, utype_5, utype_6, x.getInt(4), x.getInt(5))
      })
      .createTempView("pro_action2")

    //按照时间聚合行为数据
    sql("select userId,productId,day,first(model_id) model_id," +
      "sum(utype_1) utype_1,sum(utype_2) utype_2,sum(utype_3) utype_3,sum(utype_4)  utype_4,sum(utype_5)  utype_5,sum(utype_6) utype_6" +
      ",first(cate) cate,first(brand) brand from pro_action2 group by userId,productId,day order by day")
      .rdd
      .map(x =>
        ((x.getInt(0), x.getInt(1)),
          (x.getInt(2) /*x.getInt(3),*/
            , Array(x.getLong(4).toInt, x.getLong(5).toInt, x.getLong(6).toInt, x.getLong(7).toInt
            , x.getLong(8).toInt, x.getLong(9).toInt)))
        /*, x.getInt(10), x.getInt(11)*/)
      .groupByKey()
      .map {
        case x => {
          var map: Map[Int, Array[Int]] = Map()
          for (elem <- x._2.toList) {
            map += (elem._1 -> elem._2)
          }


          val (t1_1, t2_1, t3_1, t4_1, t5_1, t6_1) = getOneDayNum(map, end, 0)
          val (t1_2, t2_2, t3_2, t4_2, t5_2, t6_2) = getOneDayNum(map, end, 1)
          val (t1_3, t2_3, t3_3, t4_3, t5_3, t6_3) = getOneDayNum(map, end, 2)

          val (t1_l_3, t2_l_3, t3_l_3, t4_l_3, t5_l_3, t6_l_3) = getLastFullDayNum(map, end, 3)
          val (t1_l_5, t2_l_5, t3_l_5, t4_l_5, t5_l_5, t6_l_5) = getLastFullDayNum(map, end, 5)
          val (t1_l_7, t2_l_7, t3_l_7, t4_l_7, t5_l_7, t6_l_7) = getLastFullDayNum(map, end, 7)
          val (t1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10) = getLastFullDayNum(map, end, 10)
          val (t1_l_14, t2_l_14, t3_l_14, t4_l_14, t5_l_14, t6_l_14) = getLastFullDayNum(map, end, 14)
          val (t1_l_50, t2_l_50, t3_l_50, t4_l_50, t5_l_50, t6_l_50) = getLastFullDayNum(map, end, 50)

          ProUserAction3(x._1._1, x._1._2,
            t1_1, t2_1, t3_1, t4_1, t5_1, t6_1,
            t1_2, t2_2, t3_2, t4_2, t5_2, t6_2,
            t1_3, t2_3, t3_3, t4_3, t5_3, t6_3,
            t1_l_3, t2_l_3, t3_l_3, t4_l_3, t5_l_3, t6_l_3,
            t1_l_5, t2_l_5, t3_l_5, t4_l_5, t5_l_5, t6_l_5,
            t1_l_7, t2_l_7, t3_l_7, t4_l_7, t5_l_7, t6_l_7,
            t1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10,
            t1_l_14, t2_l_14, t3_l_14, t4_l_14, t5_l_14, t6_l_14,
            t1_l_50, t2_l_50, t3_l_50, t4_l_50, t5_l_50, t6_l_50
          )
        }
      }.toDF()
      .createTempView("train_features")


    //提取label
    sql(s"""select userId,productId,utype from pro_action where day >= $l_start and day<=$l_end and utype=4""")
      .distinct()
      .map(x => Label(x.getInt(0), x.getInt(1), 1))
      .createTempView("train_label")


    //组合features和label形成训练集
    sql("select f.*, ifnull(label,0) label from train_features f left join train_label l on f.userId=l.userId and f.productId=l.productId")
      .createTempView("train")

    //将训练集连接上用户表和产品表的特征，形成最终训练集，并保存
    sql("select t.userId,t.productId," +
      // "model_id," +
      //"utype_1,utype_2,utype_3,utype_4,utype_5,utype_6," +
      " a1, a2, a3,p.brand," +
      "sex_0,sex_1,sex_2,age_d1,age_1,age_2,age_3,age_4,age_5,age_6,user_lv_cd," +
      "ifnull(comment_num,0) comment_num, ifnull(has_bad_comment,0) has_bad_comment, ifnull(bad_comment_rate,0) bad_comment_rate," +
      "t1_1, t2_1, t3_1, t4_1, t5_1, t6_1,\nt1_2, t2_2, t3_2, t4_2, t5_2, t6_2,\nt1_3, t2_3, t3_3, t4_3, t5_3, t6_3,\nt1_l_3, t2_l_3, t3_l_3, t4_l_3, t5_l_3, t6_l_3,\nt1_l_5, t2_l_5, t3_l_5, t4_l_5, t5_l_5, t6_l_5,\nt1_l_7, t2_l_7, t3_l_7, t4_l_7, t5_l_7, t6_l_7,\nt1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10,\nt1_l_14, t2_l_14, t3_l_14, t4_l_14, t5_l_14, t6_l_14,\nt1_l_50, t2_l_50, t3_l_50, t4_l_50, t5_l_50, t6_l_50," +
      "label" +
      " from train t left join pro_product p on t.productId=p.productId left join pro_user u on t.userId=u.userId left join pro_comment c on t.productId=c.productId" +
      " where t.userId !=267705 and sex_0 is not null")
      .repartition(1)
      //.write.option("header", "true").mode("overwrite").parquet("train-pro")
      .show()
  }


  def getOneDayNum(map: Map[Int, Array[Int]], end: Int, day: Int) = {
    var (t1_1, t2_1, t3_1, t4_1, t5_1, t6_1) = (0, 0, 0, 0, 0, 0)
    if (map.contains(end - day)) {
      t1_1 = map(end - day)(0)
      t2_1 = map(end - day)(1)
      t3_1 = map(end - day)(2)
      t4_1 = map(end - day)(3)
      t5_1 = map(end - day)(4)
      t6_1 = map(end - day)(5)
    }
    (t1_1, t2_1, t3_1, t4_1, t5_1, t6_1)
  }

  def getLastFullDayNum(map: Map[Int, Array[Int]], end: Int, day: Int): (Int, Int, Int, Int, Int, Int) = {
    var (t1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10) = (0, 0, 0, 0, 0, 0)
    for (elem <- end - day to end) {
      if (map.contains(elem)) {
        t1_l_10 = t1_l_10 + map(elem)(0)
        t2_l_10 = t2_l_10 + map(elem)(1)
        t3_l_10 = t3_l_10 + map(elem)(2)
        t4_l_10 = t4_l_10 + map(elem)(3)
        t5_l_10 = t5_l_10 + map(elem)(4)
        t6_l_10 = t6_l_10 + map(elem)(5)
      }
    }
    (t1_l_10, t2_l_10, t3_l_10, t4_l_10, t5_l_10, t6_l_10)
  }

}
