import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random

private[spark] object dataProcessing {

  //1，case class要放在函数外面
  case class syslog1(sys_id: String, sys_hostname: String, sys_time: String, sys_message: String)

  case class syslog2(sys_id: String, sys_hostname: String, sys_time: String, sys_message: String, sys_user: String, sys_srchost: String)

  case class stds(stds_id: String, stds_srcip: String, stds_dstip: String, stds_rulename: String, stds_timedate: String)

  case class tcdns(tc_uuid: String, tc_id: String, tc_yy: String, tc_srcip: String, tc_dstip: String, tc_protocol: String, tc_score: String, tc_comment: String)

  /**
    * 逻辑处理函数
    *
    * 这里注意case class不能放在这里面，否则会报错：value toDF is not a member of org.apache.spark.rdd.RDD[syslog1]
    * 参考：https://stackoverflow.com/questions/36055774/value-todf-is-not-a-member-of-org-apache-spark-rdd-rdd
    *
    * @param dataProcessingMode 选择匹配模式,目前实现的有syslog日志、stds日志、tc日志
    * @param rdd                传入rdd
    * @param spark              sparksession
    * @return
    */
  def Porcess(dataProcessingMode: String, rdd: RDD[String], spark: SparkSession): DataFrame = {

    import spark.implicits._

    if (dataProcessingMode == "default") {
      val kuduDF = rdd.map(_.split(",")).map(p => syslog1(p(0), p(1), p(2), p(3))).toDF()
      return kuduDF
    }
    else if (dataProcessingMode == "common") {
      val schemaString = "sys_id,sys_hostname,sys_time,sys_message"
      val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
      val kuduRDD = rdd.map(_.split(",")).map(p => Row(p(0), p(1), p(2), p(3)))
      val kuduDF = spark.createDataFrame(kuduRDD, schema)
      return kuduDF
    }
    else if (dataProcessingMode == "newcommon") {
      //import spark.implicits._
      val kuduDF = rdd.map(_.split(",")).map(x =>
        /*          if (x.contains("Failed")) {
                    (x(0), x(1), x(2), x(3), x(3).substring(x(3).indexOf("for") + 4, x(3).indexOf("from") - 1), x(3).substring(x(3).indexOf("from") + 5, x(3).indexOf("port") - 1))
                  } else {
                    (x(0), x(1), x(2), x(3), null, null)
                  }*/
        (x(0), x(1), x(2), x(3), if (x(3).contains("Failed")) x(3).substring(x(3).indexOf("for") + 4, x(3).indexOf("from") - 1) else null,
          if (x(3).contains("Failed")) x(3).substring(x(3).indexOf("from") + 5, x(3).indexOf("port") - 1) else null)
      ).map(x => syslog2(x._1, x._2, x._3, x._4, x._5, x._6)).toDF()
      return kuduDF
    }
    else if (dataProcessingMode == "stds") {
      //import spark.implicits._
      val kuduDF = rdd.map(_.split(",")).map(p => stds(p(0), p(1), p(2), p(3), p(4))).toDF()
      return kuduDF
    }
    else if (dataProcessingMode == "tcdns") {
      //import spark.implicits._
      val kuduDF = rdd.map(_.split(",")).map(p => tcdns(BigInt.probablePrime(50, Random).toString(), p(0).replace("\"", ""), p(1).replace("\"", ""), p(2).replace("\"", ""), p(3).replace("\"", ""), p(4).replace("\"", ""), p(5).replace("\"", ""), p(6).replace("\"", ""))).toDF()
      return kuduDF
    }
    else {
      System.exit(1)
      return null
    }
  }

  def STDSjsonProcess(dataProcessingMode: String, rdd: RDD[String], spark: SparkSession): DataFrame = {

    import spark.implicits._
    val kuduDF = rdd.map(_.split(",")).map(p => stds(p(0), p(1), p(2), p(3), p(4))).toDF()
    return kuduDF

  }

  def TCdnsProcess(dataProcessingMode: String, rdd: RDD[String], spark: SparkSession): DataFrame = {

    import spark.implicits._
    val kuduDF = rdd.map(_.split(",")).map(p => tcdns(BigInt.probablePrime(50, Random).toString(), p(0), p(1), p(2), p(3), p(4), p(5), p(6))).toDF()
    return kuduDF

  }

  def randomString(string: String): String = {
    if (string == "string") {
      return BigInt.probablePrime(10, Random).toString(36)
    }
    else if (string == "int") {
      return BigInt.probablePrime(50, Random).toString()
    }
    else {
      return "1111"
    }
  }

}
