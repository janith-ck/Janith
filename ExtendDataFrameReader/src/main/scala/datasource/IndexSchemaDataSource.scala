package datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class IndexSchemaDataSource extends DataSourceRegister with SchemaRelationProvider{
  override def shortName(): String = "index"

  /*override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new IndexRelation(sqlContext, parameters("path"),parameters("columns"))*/
  override def createRelation(
                      sqlContext: SQLContext,
                      parameters: Map[String, String],
                      schema: StructType) : BaseRelation =
    new IndexSchemaRelation(sqlContext, parameters("path"),parameters("index_column"),schema)
}
