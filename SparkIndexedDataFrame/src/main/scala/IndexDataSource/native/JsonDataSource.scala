package IndexDataSource.native
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}

class JsonDataSource(val schema:String) extends NativeSparkDataSource {

  override def read(path: String,sqlContext: SQLContext): DataFrame = {
    val schemaStruct = new StructType()
    val cols: List[String] = schema.split(",").toList
    for ( col <- cols) {
      schemaStruct.add(col,StringType,true)
    }

    sqlContext.sparkSession.read.schema(schemaStruct).json(path).cache()
  }
}
