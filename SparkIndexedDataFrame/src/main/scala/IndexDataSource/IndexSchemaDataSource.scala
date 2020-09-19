package IndexDatasource

import java.io.File

import Ignite.IgniteSingleton
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

//class IndexSchemaDataSource extends DataSourceRegister with SchemaRelationProvider{
class IndexSchemaDataSource extends DataSourceRegister with RelationProvider{
  override def shortName(): String = "index"

  /*override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new IndexRelation(sqlContext, parameters("path"),parameters("columns"))*/

  override def createRelation(sqlContext: SQLContext,parameters: Map[String, String]) : BaseRelation = {
    val path = parameters("path")
    val sourceType = parameters("SourceFormat")
    val index_column = parameters("IndexColumn")
    var df:DataFrame = null
    IgniteSingleton.GetIgnite(sqlContext)

    val fd = new File(path)
    val fileName = FilenameUtils.getBaseName(fd.getName())

    if(sourceType=="Csv"){
      df = sqlContext.sparkSession.read.option("header",true).format("csv").load(path).cache()
      //df.createOrReplaceTempView(fileName+"_")
    }
    var c = df.schema
    var simpleSchema = new StructType(Array())

    val cols: List[String] = index_column.split(",").toList

    for ( col <- cols) {
      simpleSchema = simpleSchema.add(c.apply(col))
    }
    simpleSchema

    // split comma separate columns and create df only with index fields
    //val cols: List[String] = index_column.split(",").toList
    val col: List[Column] = cols.map(df(_))
    val indexFrame = df.select(col:_*)



    new IndexSchemaRelation(sqlContext, cols,df,path,simpleSchema,fileName)
  }
}
