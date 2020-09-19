package Ignite

import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.sql.SQLContext

object IgniteSingleton {
  val CFG_PATH = sys.env("INDEX_IGNITE_XML")
  private val MAVEN_HOME = sys.env("INDEX_MAVEN_HOME")

 // Ignition.setClientMode(true)
  private var ignite :Ignite = null

  def Init(sqlContext: SQLContext): Unit ={
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\apache\\ignite\\ignite-core\\2.8.0\\ignite-core-2.8.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\apache\\ignite\\ignite-spring\\2.8.0\\ignite-spring-2.8.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\apache\\ignite\\ignite-log4j\\2.8.0\\ignite-log4j-2.8.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\apache\\ignite\\ignite-spark-2.4\\2.8.0\\ignite-spark-2.4-2.8.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\apache\\ignite\\ignite-indexing\\2.8.0\\ignite-indexing-2.8.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\springframework\\spring-beans\\4.3.26.RELEASE\\spring-beans-4.3.26.RELEASE.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\springframework\\spring-core\\4.3.26.RELEASE\\spring-core-4.3.26.RELEASE.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\springframework\\spring-context\\4.3.26.RELEASE\\spring-context-4.3.26.RELEASE.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\org\\springframework\\spring-expression\\4.3.26.RELEASE\\spring-expression-4.3.26.RELEASE.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\javax\\cache\\cache-api\\1.0.0\\cache-api-1.0.0.jar")
    sqlContext.sparkSession.sparkContext.addJar(MAVEN_HOME + "\\com\\h2database\\h2\\1.4.197\\h2-1.4.197.jar")
    Ignition.setClientMode(true)
    ignite = Ignition.start(CFG_PATH)
  }

  def GetIgnite(sqlContext: SQLContext): Ignite = {
    if(ignite==null){
      Init(sqlContext: SQLContext)
    }
    ignite
  }
}
