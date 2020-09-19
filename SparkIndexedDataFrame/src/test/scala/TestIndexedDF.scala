import java.util.Calendar

import Ignite.IgniteSingleton

// Sample that allows to create indexed data Frame through spark in ignite
// This will read external source and write data to ignite with index
object TestIndexedDF extends App{

  // Initialize spark session in normal way
  val sparkSession = SparkConfiguration.InitializeSpark

  //Parameters
  val index_columns = "Id,UserId" //Index columns
  val source_type = IndexedFrameType.Csv //Source type
  val filenameleft = "ResearchSample10"
  val filenameright = "ResearchResponse10"
  val path = "G:\\Education\\Spark\\Janith\\ExtendDataFrameReader\\Testdata\\Morethan1M\\"

  var timer = Calendar.getInstance.getTimeInMillis()
  // Read external data source as indexed frame
  val ldf = sparkSession.read.format("index")
    .option("IndexColumn",index_columns)
    .option("SourceFormat",source_type.toString)
    .load(path+filenameleft+".csv")

  val rdf = sparkSession.read.format("index")
    .option("IndexColumn",index_columns)
    .option("SourceFormat",source_type.toString)
    .load(path+filenameright+".csv")

  ldf.show()
  rdf.show()

  println("Reading DF - top 20 " + (Calendar.getInstance.getTimeInMillis() - timer))
  timer = Calendar.getInstance.getTimeInMillis()
  ldf.show()
  rdf.show()
  println("Reading DF - top 20 - cache" + (Calendar.getInstance.getTimeInMillis() - timer))

  //join ops
  timer = Calendar.getInstance.getTimeInMillis()
  val r =  sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  //val r =  sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  r.show()
  println("Joined in 1 --- " + ( Calendar.getInstance.getTimeInMillis() - timer ) + " ms")

  timer = Calendar.getInstance.getTimeInMillis()
  val r1 =  sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  //val r1 =  sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  r1.show()
  println("Joined in 2 --- " + ( Calendar.getInstance.getTimeInMillis() - timer ) + " ms")

  timer = Calendar.getInstance.getTimeInMillis()
  val r3 =  sparkSession.sql("select * from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  //val r3 =  sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on s.UserId = r.UserId order by r.UserId")
  r3.show()
  println("Joined in 3 --- " + ( Calendar.getInstance.getTimeInMillis() - timer ) + " ms")

  SparkConfiguration.Destroy(sparkSession)


}
