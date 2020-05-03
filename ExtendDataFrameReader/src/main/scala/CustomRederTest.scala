import IndexedDataFrame.IndexedDataFrameReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomRederTest extends  App {

  val sparkconf = new SparkConf()
    .setAppName("test")
    .setMaster("spark://spr:7077")
    .setJars(Array("E:\\spark-2.4.5-bin-hadoop2.7\\jars\\mssql-jdbc-7.0.0.jre8.jar"))

  val sparkSession = SparkSession
    .builder()
    .config(sparkconf).master("spark://spr:7077")
    .getOrCreate()

  //val reader = new IndexedDataFrameReader()

  //sparkSession.read
}
