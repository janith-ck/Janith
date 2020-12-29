package IndexDataSource.native
import org.apache.spark.sql.{DataFrame, SQLContext}

class ParquetDataSource extends NativeSparkDataSource {
  override def read(path: String,sqlContext: SQLContext): DataFrame = {
    sqlContext.sparkSession.read.parquet(path).cache()
  }
}
