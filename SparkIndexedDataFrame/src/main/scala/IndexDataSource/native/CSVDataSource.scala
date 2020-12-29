package IndexDataSource.native
import org.apache.spark.sql.{DataFrame, SQLContext}

class CSVDataSource extends NativeSparkDataSource {
  override def read(path: String,sqlContext: SQLContext): DataFrame = {
    sqlContext.sparkSession.read.option("header",true).format("csv").load(path).cache()
  }
}
