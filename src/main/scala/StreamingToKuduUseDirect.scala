import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  *
  *
  * 提交命令：
  * >1,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster
  *     --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092
  *     syslognifi 192.168.3.79:7051 impala::default.tsgz_syslog StreamingToKuduUseDirect default common
  * >2,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode cluster
  *     --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092
  *     syslognifi 192.168.3.79:7051 impala::default.tsgz_syslog_new StreamingToKuduUseDirectNew newcommon newcommon
  * >3,spark2-submit --jars /root/spark-streaming-kafka-0-10_2.11-2.1.0.jar,/root/kudu-spark2_2.11-1.3.0.jar --master yarn --deploy-mode client
  *     --class KafkaKudu.StreamingToKuduUseDirect spark-learning.jar zjdw-pre0065:9092,zjdw-pre0066:9092,zjdw-pre0067:9092,zjdw-pre0068:9092,zjdw-pre0069:9092
  *     syslognifi 192.168.3.79:7051 impala::default.tsgz_test StreamingToKuduUseDirectCheckpoint default dd_test
  */
object StreamingToKuduUseDirect extends Serializable {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <masterList> is a list of kudu
           |  <kuduTableName> is a name of kudu
           |  <appName>  is a name of spark processing
           |  <dataProcessingMode> the function of dataProcessing logical processing mode
           |         default,common,newcommon,stds,tcdns
           |  <groupid> is the name of kafka groupname
           |  <checkpoint_path> is a address of checkpoint  hdfs:///tmp/checkpoint_
        """.stripMargin)
      System.exit(1)
    }

    //1.获取输入参数与定义全局变量,主要两部分，一个brokers地址，一个topics列表，至于其他优化参数后续再加
    //TODO: 注意：后面可以改为properties的方式来获取参数
    val Array(brokers, topic, masterList, kuduTableName, appName, dataProcessingMode, groupid, checkpoint_path) = args
    val sctime = 10

    //2.配置spark环境以及kudu环境
    val spark = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(sctime))

    ssc.checkpoint(checkpoint_path)

    val kuduContext = new KuduContext(masterList, spark.sparkContext)

    //3.配置创建kafka输入流的所需要的参数，注意这里可以加入一些优化参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupid,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array(topic)

    //4.创建kafka输入流0.10
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>

      //获取所有的value
      val messageRDD = rdd.map(record => (record.key(), record.value())).values

      val kuduDF = dataProcessing.Porcess(dataProcessingMode, messageRDD, spark)

      if (kuduDF == null) {
        System.err.println("dataProcessingMode选择错误： " + dataProcessingMode + "\n" +
          "dataProcessingMode可以为default,common,newcomer")
        System.exit(1)
      }
      kuduContext.upsertRows(kuduDF, kuduTableName)

    }

    //6.Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
