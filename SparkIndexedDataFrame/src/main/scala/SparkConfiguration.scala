import Ignite.IgniteSingleton
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

//Initialize spark session
// This is to maintain common place to connect spark
object SparkConfiguration {
  private val masterUrl = "spark://127.0.0.10:7077"
  private val appName = "IndexDataFrameTest"

  //Create spark connection
  def InitializeSpark: SparkSession = {

    type ExtensionsBuilder = SparkSessionExtensions => Unit

    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(masterUrl)

    val sparkSession = SparkSession
      .builder()
      .config(sparkConf).master(masterUrl)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  //destroy spark connection
  def Destroy(session:SparkSession):Unit = {
    val ignite = IgniteSingleton.GetIgnite(session.sqlContext)
    ignite.close()
    session.stop()
  }



}
