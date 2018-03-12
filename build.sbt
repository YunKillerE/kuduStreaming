name := "kuduStreaming"

version := "0.1"

scalaVersion := "2.11.11"

javacOptions++=Seq("-source","1.8","-target","1.8")


val kafkaVersion = "0.10.1.0"
val sparkVersion = "2.2.1"
val kuduVersion = "1.4.0"


libraryDependencies ++= Seq(
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.kudu" % "kudu-client" % kuduVersion,
  "org.apache.kudu" %% "kudu-spark2" % kuduVersion,
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
)