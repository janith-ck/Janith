import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestPeoSparkforOutputforReport extends App {
  private val masterUrl = "spark://127.0.0.10:7077"
  private val appName = "IndexDataFrameTest1"
  val filenameleft = "ResearchSample10"
  val filenameright = "ResearchResponse10"
  val path = "D:\\Education\\Spark\\Testdata\\"

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

  println("Reading DF - top 20 - cache" + (Calendar.getInstance.getTimeInMillis() - timer))

  df.createOrReplaceTempView(filenameleft)
  df1.createOrReplaceTempView(filenameright)

  timer = Calendar.getInstance.getTimeInMillis()
  //val r = sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  val r = sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId and s.ProjectId = r.ProjectId order by r.UserId")
  r.show()
  println("Joined in 1---" + (Calendar.getInstance.getTimeInMillis() - timer))

  scala.io.StdIn.readLine()
}
