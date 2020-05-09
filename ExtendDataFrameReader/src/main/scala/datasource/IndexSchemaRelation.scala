package datasource

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class IndexSchemaRelation(val sqlContext: SQLContext, val path: String, val index_column:String, val sc :StructType) extends BaseRelation with TableScan{
  print("index_column >>>>>>>>> " + index_column)
  override def schema:  StructType = sc

  override def buildScan(): RDD[Row] = {
    println("In IndexSchemaRelation.............")
    val df = sqlContext.sparkSession.read.format("csv").load(path)
    //df.show()
    df.rdd
  }
}
