import java.util.Calendar

import Ignite.IgniteSingleton
import org.apache.ignite.Ignition
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_SCHEMA, OPTION_TABLE}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

// Sample that allows to create indexed data Frame through spark in ignite
// This will read external source and write data to ignite with index
object TestIndexedDataFrameRead extends App{

  // Initialize spark session in normal way
  val sparkSession = SparkConfiguration.InitializeSpark

  //Input data
  //1. Index columns -
  val index_columns = "Id,UserId"
  //2. Source type -
  val source_type = IndexedFrameType.Csv
  //3. Source file path
  val source_path_small = "G:\\Education\\Spark\\Janith\\ExtendDataFrameReader\\Research_Sample_small.csv"
  val source_path1_samll = "G:\\Education\\Spark\\Janith\\ExtendDataFrameReader\\Research_Response1_small.csv"

  // Read external data source as indexed frame
  val df = sparkSession.read.format("index")
    .option("IndexColumn",index_columns)
    .option("SourceFormat",source_type.toString)
    .load(source_path_small)

  val df1 = sparkSession.read.format("index")
    .option("IndexColumn",index_columns)
    .option("SourceFormat",source_type.toString)
    .load(source_path1_samll)


  df.show()
  df1.show()

 /* var resultDF = sparkSession.read
    .format(FORMAT_IGNITE)               // Data source type.
    .option(OPTION_TABLE, "Research_Sample_Medium")      // Table to read.
    .option(OPTION_CONFIG_FILE, IgniteSingleton.CFG_PATH) // Ignite config.
    .option(OPTION_SCHEMA, "PUBLIC")
    .load()

  var resultDF1 = sparkSession.read
    .format(FORMAT_IGNITE)               // Data source type.
    .option(OPTION_TABLE, "Research_Response1_Medium")      // Table to read.
    .option(OPTION_CONFIG_FILE, IgniteSingleton.CFG_PATH) // Ignite config.
    .option(OPTION_SCHEMA, "PUBLIC")
    .load()
*/
  //Display dataframe
  println("Start from spark---")
 // resultDF.show();
 // resultDF1.show();


  //resultDF.createOrReplaceTempView("Research_Sample_Medium")
  //resultDF1.createOrReplaceTempView("Research_Response1_Medium")
  var d = Calendar.getInstance.getTimeInMillis()
  val r =  sparkSession.sql("select r.* from Research_Sample_small s join Research_Response1_small r on s.UserId = r.UserId order by r.UserId")
 // r.createOrReplaceTempView("Tempout")
 // val finalo = sparkSession.sql("select * from Research_Sample_Medium_ where UserId in (select UserId from Tempout)")
 // finalo.show()
  r.show()
  println("Now load from Ignite---" + ( Calendar.getInstance.getTimeInMillis() - d ))
  //d = Calendar.getInstance.getTimeInMillis()
 // df.show()
 // println("ignite done---"+ ( Calendar.getInstance.getTimeInMillis() - d ))

  //df1.createOrReplaceTempView("Research_Response1_small")

  //val r =  sparkSession.sql("select * from Research_Sample_small s join Research_Response1_small r on s.UserId = r.UserId")

  //r.explain()

  //d = Calendar.getInstance.getTimeInMillis()



  //resultDF.show()
 // println("Now load from Ignite direct---" + ( Calendar.getInstance.getTimeInMillis() - d ))*/

  //resultDF.show()

 // ignite.close()
  println("End from ignite---")

  val ignite = IgniteSingleton.GetIgnite(sparkSession.sqlContext)
  ignite.close()
  //Destroy spark connection
  SparkConfiguration.Destroy(sparkSession)


}
