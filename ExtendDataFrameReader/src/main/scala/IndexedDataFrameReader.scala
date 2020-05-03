package IndexedDataFrame

  import org.apache.spark.sql.DataFrameReader

  class IndexedDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def IndexRead(indexCol: String, path: String, format: String) = reader.format(format).load(path)
  }




