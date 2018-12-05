package com.lxf.jdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object AnalyseData {

  case class User(userId: Int, age: Int, sex_0: Int, sex_1: Int, sex_2: Int,
                  age_d1: Int, age_1: Int, age_2: Int, age_3: Int, age_4: Int, age_5: Int, age_6: Int,
                  user_lv_cd: Int, user_reg_tm: String)

  case class Product(productId: Int, a1: Int, a2: Int, a3: Int, brand: Int)

  //cate无意义，删去
  case class Comment(dt: String, productId: Int, comment_num: Int, has_bad_comment: Int, bad_comment_rate: Double)

  case class UserAction(userId: Int, productId: Int, time: String, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction(userId: Int, productId: Int, day: Int, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction2(userId: Int, productId: Int, day: Int, model_id: Int,
                            utype_1: Int, utype_2: Int, utype_3: Int, utype_4: Int, utype_5: Int, utype_6: Int,
                            cate: Int, brand: Int)

  case class Train(userId: Int, productId: Int, model_id: Int, utype_1: Int, utype_2: Int, utype_3: Int, utype_4: Int, utype_5: Int, utype_6: Int, cate: Int, brand: Int, label: Int)

  var count = 0

  def main(args: Array[String]): Unit = {
    SetLogger
    val conf = new SparkConf().setMaster("local[10]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    import spark.sql

    sql("use jdata")

    //用户表
    println("------------用户表数据------------")
    sql("select * from user").show()
    println("用户表年龄统计"   )
    sql("select age,count(1) from user group by age").show()
    println("用户表性别统计"   )
    sql("select sex,count(1) from user group by sex").show()
    println("用户表年龄不为空值")
    sql("select * from user where age ='NULL'").show()
    println("用户表用户等级统计")
    sql("select user_lv_cd,count(1) from user group by user_lv_cd").show()
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
    println("清洗后的用户表")
    sql("select * from pro_user").show()



    println("------------产品表数据------------")

    //
    //    //商品表
    println("数据")
    sql("select * from product").show()
    println("a1统计")
    sql("select a1,count(1) from product group by a1").show()
    println("a2统计")
    sql("select a2,count(1) from product group by a2").show()
    println("a3统计")
    sql("select a3,count(1) from product group by a3").show()
    println("cate品类统计")
    sql("select cate,count(1) from product group by cate").show()
    println("brand品牌统计")
    sql("select brand,count(1) from product group by brand").show()

    sql("select * from product")
      .map(x => Product(x.getString(0).toInt, x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt, x.getString(5).toInt))
      .createTempView("pro_product")
    println("清洗后的产品表")
    sql("select * from pro_product").show()


    println("------------评论表数据------------")

    //    //评论表
    println("数据")
    sql("select * from comment").show()
    println("日期统计")
    sql("select dt,count(1) from comment group by dt").show()
    println("数量统计")
    sql("select count(*) from comment").show()
    println("有效评论数量")
    sql("select count(*) from comment c left join product p on c.sku_id=p.sku_id").show()
    println("商品评论数量统计")
    sql("select sku_id,count(*) from comment group by sku_id").show()
    println("评论数统计")
    sql("select comment_num,count(*) from comment group by comment_num").show()
    println("是否有差评统计")
    sql("select has_bad_comment,count(*) from comment group by has_bad_comment").show()
    println("差评率统计")
    sql("select bad_comment_rate,count(*) count from comment group by bad_comment_rate order by count").show()
    sql("select * from comment")
      .map(x => Comment(x.getString(0), x.getString(1).toInt, x.getString(2).toInt, x.getString(3).toInt, x.getString(4).toDouble))
      .createTempView("pro_comment")
    println("清洗后的评论表")
    sql("select * from pro_comment").show()


    //用户行为表
    println("------------用户行为表数据------------")
    println("数据")
    sql("select * from user_action").show()
    println("数据总数")
    sql("select count(1) from user_action").show()
    println("用户存在的行为总数")
    sql("select count(1) from user_action ua left join user u on ua.user_id=u.user_id").show()
    println("产品存在的行为总数")
    sql("select count(1) from user_action ua  join product p on ua.sku_id=p.sku_id").show()
    println("model_id统计")
    sql("select model_id,count(1) from user_action group by model_id order by count(1) desc").show()
    println("type统计")
    sql("select type ,count(1) from user_action group by type").show()
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

    //    sql("select * from pro_action")
    sql("select cate,count(1) from pro_action group by cate").show()
    //          sql("select day,count(1) from actionplus group by day").show()
    println("所有用户行为")
    sql("select day,count(1) from pro_action group by day order by day").show()
    println("浏览")
    sql("select day,count(1) from pro_action where utype=1 group by day order by day").show()
    println("加购")
    sql("select day,count(1) from pro_action where utype=2 group by day order by day").show()
    println("删除")
    sql("select day,count(1) from pro_action where utype=3 group by day order by day").show()
    println("下单")
    sql("select day,count(1) from pro_action where utype=4 group by day order by day").show()
    println("关注")
    sql("select day,count(1) from pro_action where utype=5 group by day order by day").show()
    println("点击")
    sql("select day,count(1) from pro_action where utype=6 group by day order by day").show()


    //开始抽取特征
    sql("select * from pro_action where day >= 0 and day<=1000")
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
    println("清洗后的用户行为表")

    sql("select * from pro_action2").show()

    sql("select day, sum(utype_1),sum(utype_2),sum(utype_3),sum(utype_4),sum(utype_5),sum(utype_6) from pro_action2  group by day order by day").show(200)


    sql("select userId,productId,first(model_id) model_id," +
      "sum(utype_1) utype_1,sum(utype_2) utype_2,sum(utype_3) utype_3,sum(utype_4)  utype_4,sum(utype_5)  utype_5,sum(utype_6) utype_6" +
      ",first(cate) cate,first(brand) brand from pro_action2 group by userId,productId")
      .createTempView("train_features")

    sql("select userId,productId,utype from pro_action where day >= 36 and day<=40 and utype=4").distinct()
      .createTempView("train_label")
    //sql("select * from train_label").show()

    sql("select * from train_features f left join train_label l on f.userId=l.userId and f.productId=l.productId")
      .createTempView("train")
    sql("select * from train")
      .map(x => {
        val label = x.get(13) match {
          case None => 0
          case 4 => 1
          case _ => 0
        }
        Train(x.getInt(0), x.getInt(1), x.getInt(2), x.getLong(3).toInt, x.getLong(4).toInt, x.getLong(5).toInt, x.getLong(6).toInt, x.getLong(7).toInt, x.getLong(8).toInt, x.getInt(9), x.getInt(10), label)
      }).createTempView("pro_train")

  }


  def SetLogger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    System.setProperty("spark.ui.showConsoleProgress", "false")
    Logger.getRootLogger().setLevel(Level.OFF);
  }




}
