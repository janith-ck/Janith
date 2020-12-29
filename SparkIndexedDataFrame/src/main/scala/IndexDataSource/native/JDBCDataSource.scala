package IndexDataSource.native
import org.apache.spark.sql.{DataFrame, SQLContext}
import java.util.Properties

class JDBCDataSource extends NativeSparkDataSource {
  override def read(path: String,sqlContext: SQLContext): DataFrame = {
    val jdbcHostname = "<hostname>"
    val jdbcPort = 1433
    val jdbcDatabase = "<database>"
    val jdbcUsername = "<database>"
    val jdbcPassword = "<database>"

    // Create the JDBC URL without passing in the user and password parameters.
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}"

    // Create a Properties() object to hold the parameters.

    val connectionProperties = new Properties()

    connectionProperties.put("user", s"${jdbcUsername}")
    connectionProperties.put("password", s"${jdbcPassword}")

    val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connectionProperties.setProperty("Driver", driverClass)

    sqlContext.sparkSession.read.jdbc(jdbcUrl,"",connectionProperties).cache()
  }
}
