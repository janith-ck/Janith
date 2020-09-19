import java.util.Calendar

import SparkConfiguration.{appName, masterUrl}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestPeoSpark extends App {
  private val masterUrl = "spark://127.0.0.10:7077"
  private val appName = "IndexDataFrameTest1"
  val filenameleft = "ResearchSample10"
  val filenameright = "ResearchResponse10"
  val path = "G:\\Education\\Spark\\Janith\\ExtendDataFrameReader\\Testdata\\"

  val sparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(masterUrl)

  val sparkSession = SparkSession
    .builder()
    .config(sparkConf).master(masterUrl)
    .getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  var timer = Calendar.getInstance.getTimeInMillis()
  val df = sparkSession.read.option("header", true).format("csv").load(path+filenameleft+".csv").cache()
  val df1 = sparkSession.read.option("header", true).format("csv").load(path+filenameright+".csv").cache()

  df.show()
  df1.show()

  println("Reading DF - top 20 " + (Calendar.getInstance.getTimeInMillis() - timer))
  timer = Calendar.getInstance.getTimeInMillis()
  df.show()
  df1.show()
  println("Reading DF - top 20 - cache" + (Calendar.getInstance.getTimeInMillis() - timer))

  df.createOrReplaceTempView(filenameleft)
  df1.createOrReplaceTempView(filenameright)

  timer = Calendar.getInstance.getTimeInMillis()
  //val r = sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  val r = sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  r.show()
  println("Joined in 1---" + (Calendar.getInstance.getTimeInMillis() - timer))

  timer = Calendar.getInstance.getTimeInMillis()
  //val r2 = sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  val r2 = sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  r2.show()
  println("Joined in 2---" + (Calendar.getInstance.getTimeInMillis() - timer))

  timer = Calendar.getInstance.getTimeInMillis()
  //val r3 = sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  val r3 = sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  r3.show()
  println("Joined in 3---" + (Calendar.getInstance.getTimeInMillis() - timer))

  scala.io.StdIn.readLine()
}
