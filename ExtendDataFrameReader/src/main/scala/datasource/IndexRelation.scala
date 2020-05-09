package datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class IndexRelation(val sqlContext: SQLContext, val path: String, val columns:String) extends BaseRelation with TableScan{
  print("thisiiiiiiiiiii " + columns)
  override def schema:  StructType = StructType(Seq(
    StructField("Id", StringType, false),
    StructField("UserId", StringType, false),
    StructField("ProjectId", StringType, false)
    ))

  override def buildScan(): RDD[Row] = {
    println("In IndexRelation .............")
    val df = sqlContext.sparkSession.read.format("csv").load(path)
    //df.show()
    df.rdd
  }
}
