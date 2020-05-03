package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, PrunedFilteredScan, TableScan}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

class LogRelation(val sqlContext: SQLContext, val path: String) extends BaseRelation with TableScan{
  override def schema: StructType = StructType(Seq(
    StructField("Id", StringType, false),
    StructField("UserId", StringType, false),
    StructField("ProjectId", StringType, false)
    ))

  override def buildScan(): RDD[Row] = {
    val df = sqlContext.sparkSession.read.format("csv").load(path)
    df.show()
    df.rdd
  }
}
