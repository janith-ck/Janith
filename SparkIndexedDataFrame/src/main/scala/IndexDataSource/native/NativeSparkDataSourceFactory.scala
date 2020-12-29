package IndexDataSource.native

object NativeSparkDataSourceFactory {

  //allcolumns is needed when schema of datasource can not be defined type itself ex: json
  def getDataSource(sourceType:String,allcolumns:String):NativeSparkDataSource = sourceType.toUpperCase match {
    case "CSV" => new CSVDataSource()
    case "JSON" => new JsonDataSource(allcolumns)
    case "PARQUET" => new ParquetDataSource()
    case "JDBC" => new JDBCDataSource()
  }
}
