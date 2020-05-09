package datasource

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class IndexDataSource extends DataSourceRegister with RelationProvider{
  override def shortName(): String = "index"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new IndexRelation(sqlContext, parameters("path"),parameters("columns"))
}
