import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object IndexTestReader extends  App {
  val sparkconf = new SparkConf()
    .setAppName("test")
    .setMaster("spark://spr:7077")

  val sparkSession = SparkSession
    .builder()
    .config(sparkconf).master("spark://spr:7077")
    .getOrCreate()


  //val df = sparkSession.read.format("index").option("columns","sample").load("Research_Sample_small.csv")
  val df = sparkSession.read.format("index")
    .option("index_column","UserId,ProjectId")
    .schema(StructType(Seq(
      StructField("Id", StringType, false),
      StructField("UserId", StringType, false),
      StructField("ProjectId", StringType, false)
    )))
    .load("Research_Sample_small.csv")
  df.show()
  println("Done....")

  sparkSession.close()
}
