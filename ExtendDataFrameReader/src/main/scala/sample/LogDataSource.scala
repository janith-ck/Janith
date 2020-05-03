package sample

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class LogDataSource extends DataSourceRegister with RelationProvider{
  override def shortName(): String = "log"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    new LogRelation(sqlContext, parameters("path"))
}
