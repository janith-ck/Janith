package sample

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestReader extends  App {
  val sparkconf = new SparkConf()
    .setAppName("test")
    .setMaster("spark://spr:7077")
    //.setJars(Array("G:\\Education\\Spark\\Janith\\ExtendDataFrameReader\\out\\artifacts\\ExtendDataFrameReader_jar\\ExtendDataFrameReader.jar"))

  val sparkSession = SparkSession
    .builder()
    .config(sparkconf).master("spark://spr:7077")
    .getOrCreate()

  //new LogRelation(sparkSession.sqlContext, "G:\\Education\\Spark\\SampleCSV\\Research_Sample_small.csv").buildScan().foreach(println)

  val df = sparkSession.read.format("log").load("Research_Sample_small.csv")
  df.show()
  println("Done....")

  sparkSession.close()
}
