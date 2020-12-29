package IndexDataSource.native

import org.apache.spark.sql.{DataFrame, SQLContext}

trait NativeSparkDataSource {
  def read(path:String,sqlContext: SQLContext):DataFrame
}
