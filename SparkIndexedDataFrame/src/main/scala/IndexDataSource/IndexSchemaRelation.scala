package IndexDatasource

import java.io.File
import java.util.Calendar

import Ignite.ManageIgnite
import org.apache.commons.io.FilenameUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}

class IndexSchemaRelation(val sqlContext: SQLContext,  val cols:List[String], val df:DataFrame,
                          val path:String,val sch:StructType,val fileName:String) extends BaseRelation with TableScan{


  override def schema = sch

  override def buildScan(): RDD[Row] = {
    var ig = new ManageIgnite()
    ig.SaveIndexToIgnite(df,fileName,sqlContext,cols)
  }
}
