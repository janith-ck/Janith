package Ignite

import java.util.Calendar

import org.apache.ignite.Ignition
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteDataFrameSettings.{FORMAT_IGNITE, OPTION_CONFIG_FILE, OPTION_CREATE_TABLE_PARAMETERS, OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, OPTION_SCHEMA, OPTION_STREAMER_ALLOW_OVERWRITE, OPTION_TABLE}
import org.apache.jute.compiler.{JLong, JString}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class ManageIgnite {

  private val CFG_PATH = sys.env("INDEX_IGNITE_XML") /* "G:\\Education\\Spark\\Janith\\SparkIndexedDataFrame\\ignite.xml"*/
  private val MAVEN_HOME = sys.env("INDEX_MAVEN_HOME")  /*"C:\\Users\\Janith\\.m2\\repository"*/

  def SaveIndexToIgnite(df:DataFrame,tableName:String,sqlContext: SQLContext,index_col: List[String]):RDD[Row] ={

    val ignite = IgniteSingleton.GetIgnite(sqlContext)
    if(!ignite.cacheNames().contains(tableName)) {
      df.write.mode("overwrite").
        format(FORMAT_IGNITE).
        option(OPTION_CONFIG_FILE, CFG_PATH).
        option(OPTION_SCHEMA, "PUBLIC").
        option(OPTION_TABLE, tableName).
        option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "UserId").
        option(OPTION_STREAMER_ALLOW_OVERWRITE, true).
        option(OPTION_CREATE_TABLE_PARAMETERS, "backups=0").
        save()
      val CACHE_NAME = tableName

      val ccfg = new CacheConfiguration[JLong, JString](CACHE_NAME).setSqlSchema("PUBLIC")
      val cache = ignite.getOrCreateCache(ccfg)


      cache.query(new SqlFieldsQuery("CREATE INDEX on " + tableName + " (" + index_col.mkString(",") + ")")).getAll
      //cache.query(new SqlFieldsQuery("CREATE INDEX on "+tableName+" (Id asc)")).getAll

      // igniteSession.sqlContext.sql("CREATE INDEX IF NOT EXISTS "+tableName+"_IDX ON \"PUBLIC\"."+tableName+"("+index_col(0)+")")


      println("tabled saved " + tableName)
    }
    var d = Calendar.getInstance.getTimeInMillis()
    var resultDF = sqlContext.sparkSession.read
        .format(FORMAT_IGNITE)               // Data source type.
        .option(OPTION_TABLE, tableName)      // Table to read.
        .option(OPTION_CONFIG_FILE, CFG_PATH) // Ignite config.
        .option(OPTION_SCHEMA, "PUBLIC")
        .load()

    resultDF.createOrReplaceTempView(tableName)

   resultDF.rdd
  }
}
