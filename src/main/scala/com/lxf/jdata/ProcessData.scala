package com.lxf.jdata

import com.lxf.jdata.ProcessData.ProUserAction
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{Row, SparkSession}

object ProcessData {

  case class User(userId: Int, age: Int, sex_0: Int,sex_1: Int,sex_2: Int,
                  age_d1: Int,age_1: Int,age_2: Int,age_3: Int,age_4: Int,age_5: Int,age_6: Int,
                  user_lv_cd: Int, user_reg_tm: String)

  case class Product(productId: Int, a1: Int, a2: Int, a3: Int, brand: Int)

  //cate无意义，删去
  case class Comment(dt: String, productId: Int, comment_num: Int, has_bad_comment: Int, bad_comment_rate: Double)

  case class UserAction(userId: Int, productId: Int, time: String, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction(userId: Int, productId: Int, day: Int, model_id: Int, utype: Int, cate: Int, brand: Int)

  case class ProUserAction2(userId: Int, productId: Int, day: Int, model_id: Int,
                            utype_1: Int, utype_2: Int,utype_3: Int,utype_4: Int,utype_5: Int,utype_6: Int,
                            cate: Int, brand: Int)
  case class Train(userId:Int,productId:Int,model_id:Int,utype_1:Int,utype_2:Int,utype_3:Int,utype_4:Int,utype_5:Int,utype_6:Int,cate:Int,brand:Int,label:Int)

  def main(args: Array[String]): Unit = {
    val end=101
    val (f_start,f_end,l_start,l_end)=(end-50,end,end+1,end+5)
    val conf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.sql
    import spark.implicits._

    sql("use jdata")

    //    //用户表
    //    sql("select * from user").show()
    //    sql("select age,count(1) from user group by age").show()
    //    sql("select sex,count(1) from user group by sex").show()
    //    sql("select * from user where age ='NULL'").show()
    //    sql("select user_lv_cd,count(1) from user group by user_lv_cd").show()
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
            var (age_d1,age_1,age_2,age_3,age_4,age_5,age_6)=(0,0,0,0,0,0,0)
            pro_age match {
              case 1 =>age_1=1
              case 2 =>age_2=1
              case 3 =>age_3=1
              case 4 =>age_4=1
              case 5 =>age_5=1
              case 6 =>age_6=1
              case -1 => age_d1=1
              case _=>
            }
            var (sex_0,sex_1,sex_2)=(0,0,0)
            x.getString(2).toInt match {
              case 0=>sex_0=1
              case 1=>sex_1=1
              case 2=>sex_2=1
              case _=>sex_2=1
            }
            User(x.getString(0).toInt, pro_age,
              sex_0,sex_1,sex_2,
              age_d1,age_1,age_2,age_3,age_4,age_5,age_6,
              x.getString(3).toInt, x.getString(4))
          }
          .createTempView("pro_user")
    //
    //    //商品表
    //    sql("select * from product").show()
    //    sql("select a1,count(1) from product group by a1").show()
    //    sql("select a2,count(1) from product group by a2").show()
    //    sql("select a3,count(1) from product group by a3").show()
//        sql("select cate,count(1) from product group by cate").show()
//        sql("select brand,count(1) from product group by brand").show()
    //
        sql("select * from product")
          .map(x=>Product(x.getString(0).toInt,x.getString(1).toInt,x.getString(2).toInt,x.getString(3).toInt,x.getString(5).toInt))
            .createTempView("pro_product")

    //    //评论表
    //    sql("select * from comment").show()
    //    sql("select dt,count(1) from comment group by dt").show()
    //    sql("select count(*) from comment").show()
    //    sql("select count(*) from comment c left join product p on c.sku_id=p.sku_id").show()
    //    sql("select sku_id,count(*) from comment group by sku_id").show()
    //    sql("select comment_num,count(*) from comment group by comment_num").show()
    //    sql("select has_bad_comment,count(*) from comment group by has_bad_comment").show()
    //    sql("select bad_comment_rate,count(*) count from comment group by bad_comment_rate order by count").show()
        sql("select dt, sku_id, comment_num, has_bad_comment, bad_comment_rate from comment")
          .map(x=>Comment(x.getString(0),x.getString(1).toInt,x.getString(2).toInt,x.getString(3).toInt,x.getString(4).toDouble))
      .createTempView("temp_comment")

    sql("select productId, sum(comment_num) comment_num, sum(has_bad_comment) has_bad_comment, avg(bad_comment_rate) bad_comment_rate from temp_comment group by productId")
        .createTempView("pro_comment")
    //用户行为表
//      sql("select * from user_action").show()
//      sql("select count(1) from user_action").show()
//      sql("select count(1) from user_action ua left join user u on ua.user_id=u.user_id").show()
//      sql("select count(1) from user_action ua  join product p on ua.sku_id=p.sku_id").show()
//      sql("select model_id,count(1) from user_action group by model_id order by count(1) desc").show()
//      sql("select type ,count(1) from user_action group by type").show()
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
            case "04" => 31 + 29 + 31 + date(2).toInt  //要预测的日期是 31+19+31+16 - 31+19+31+20 之间
          }
          ProUserAction(x.getString(0).toDouble.toInt, x.getString(1).toInt, day, modelId, x.getString(4).toInt, x.getString(5).toInt, x.getString(6).toInt)
        }
      }
      .createTempView("pro_action")
    sql("select * from pro_action")
//    sql("select cate,count(1) from pro_action group by cate").show()
//      sql("select day,count(1) from actionplus group by day").show()
//      sql("select day,count(1) from pro_action group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=1 group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=2 group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=3 group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=4 group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=5 group by day order by day").show(100)
//      sql("select day,count(1) from pro_action where utype=6 group by day order by day").show(100)



      //开始抽取特征
      sql(s"""select * from pro_action where day >= $f_start and day<=$f_end""")
      .map(x=>{
        var (utype_1, utype_2,utype_3,utype_4,utype_5,utype_6)=(0,0,0,0,0,0)
        x.getInt(4) match {
          case 1=>utype_1=1
          case 2=>utype_2=1
          case 3=>utype_3=1
          case 4=>utype_4=1
          case 5=>utype_5=1
          case 6=>utype_6=1
        }
        ProUserAction2(x.getInt(0),x.getInt(1),x.getInt(2),x.getInt(3),
          utype_1, utype_2,utype_3,utype_4,utype_5,utype_6,x.getInt(4),x.getInt(5))})
        .createTempView("pro_action2")

    sql("select userId,productId,first(model_id) model_id," +
      "sum(utype_1) utype_1,sum(utype_2) utype_2,sum(utype_3) utype_3,sum(utype_4)  utype_4,sum(utype_5)  utype_5,sum(utype_6) utype_6" +
      ",first(cate) cate,first(brand) brand from pro_action2 group by userId,productId")
      .createTempView("train_features")

    sql(s"""select userId,productId,utype from pro_action where day >= $l_start and day<=$l_end and utype=4""").distinct()
      .createTempView("train_label")
    //sql("select * from train_label").show()

    sql("select * from train_features f left join train_label l on f.userId=l.userId and f.productId=l.productId")
      .createTempView("train")
    sql("select * from train")
      .map(x=>{
        val label=x.get(13) match {
          case None=>0
          case 4=>1
          case _=>0
        }
        Train(x.getInt(0),x.getInt(1),x.getInt(2),x.getLong(3).toInt,x.getLong(4).toInt,x.getLong(5).toInt,x.getLong(6).toInt,x.getLong(7).toInt,x.getLong(8).toInt,x.getInt(9),x.getInt(10),label)
      }).createTempView("pro_train")
    sql("select t.userId,t.productId,model_id," +
      "utype_1,utype_2,utype_3,utype_4,utype_5,utype_6," +
      " a1, a2, a3,p.brand," +
      "sex_0,sex_1,sex_2,age_d1,age_1,age_2,age_3,age_4,age_5,age_6,user_lv_cd," +
      "ifnull(comment_num,0) comment_num, ifnull(has_bad_comment,0) has_bad_comment, ifnull(bad_comment_rate,0) bad_comment_rate," +
      "label" +
      " from pro_train t left join pro_product p on t.productId=p.productId left join pro_user u on t.userId=u.userId left join pro_comment c on t.productId=c.productId" +
      " where t.userId !=267705 and sex_0 is not null")
      .repartition(1)
      //.write.option("header","true").mode("overwrite").parquet("train")
      .show()
  }

}
