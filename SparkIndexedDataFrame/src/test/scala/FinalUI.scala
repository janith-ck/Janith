import java.util.Calendar

import scala.swing._
import scala.swing.event.ButtonClicked

object FinalUI extends App {

  val sparkSession = SparkConfiguration.InitializeSpark

  val frame = new MainFrame{

    title = "Janith - 2018535"

    val headdimention =new Dimension(400,50)

    // top label
    val nativeTextlabel = new Label {
      text = "<html>Native Spark dataframe <html>"
    }
    nativeTextlabel.horizontalAlignment = Alignment.Left
    nativeTextlabel.maximumSize = headdimention
    nativeTextlabel.preferredSize = headdimention
    nativeTextlabel.minimumSize = headdimention
    nativeTextlabel.font = new Font("Ariel", java.awt.Font.ITALIC, 20)

    val bdimention =new Dimension(75,25)
    //val ldimention =new Dimension(400,200)
    //native Load row
    val nativeload = Button("Load"){
    }
    nativeload.name = "nativeload"
    nativeload.maximumSize = bdimention
    nativeload.preferredSize = bdimention
    nativeload.minimumSize = bdimention

    val nativeloadlabel = new Label {
      text = "<html> No Results </html>"
    }

   // nativeloadlabel.maximumSize = ldimention
   // nativeloadlabel.preferredSize = ldimention
   // nativeloadlabel.minimumSize = ldimention

    //native JOIN row
    val nativejoin = Button("Join"){

    }
    nativejoin.name = "nativejoin"
    nativejoin.maximumSize = bdimention
    nativejoin.preferredSize = bdimention
    nativejoin.minimumSize = bdimention

    val nativejoinlabel = new Label {
      text = "<html> No Results </html>"
    }


    val r1 = new BoxPanel(Orientation.NoOrientation){
      contents+=nativeTextlabel
    }

    val x =new Dimension(400,60)

    val r2 = new BoxPanel(Orientation.Horizontal){
      contents+= nativeload
      contents+= nativeloadlabel
    }
    r2.maximumSize = x
    r2.preferredSize = x
    r2.minimumSize =x

    val r3 = new BoxPanel(Orientation.Horizontal){
      contents+=nativejoin
      contents+=nativejoinlabel
    }

    r3.maximumSize = x
    r3.preferredSize = x
    r3.minimumSize =x

    val topcontents = new BoxPanel(Orientation.Vertical){
      contents+=r1
      contents+=r2
      contents+=r3
    }

    //topcontents.border = Swing.EtchedBorder


    ///////////////////// bottom///////////

    val headbdimention =new Dimension(400,40)

    // top label
    val indexTextlabel = new Label {
      text = "New Indexed spark dataframe"
    }
    indexTextlabel.horizontalAlignment = Alignment.Left
    indexTextlabel.maximumSize = headbdimention
    indexTextlabel.preferredSize = headbdimention
    indexTextlabel.minimumSize = headbdimention
    indexTextlabel.font = new Font("Ariel", java.awt.Font.ITALIC, 20)


    //native Load row
    val indexload = Button("Load"){
    }
    indexload.name = "indexload"
    indexload.maximumSize = bdimention
    indexload.preferredSize = bdimention
    indexload.minimumSize = bdimention

    val indexloadlabel = new Label {
      text = "<html> No Results </html>"
    }

    //index JOIN row
    val indexjoin = Button("Join"){}
    indexjoin.name = "indexjoin"
    indexjoin.maximumSize = bdimention
    indexjoin.preferredSize = bdimention
    indexjoin.minimumSize = bdimention

    val indexjoinlabel = new Label {
      text = "<html> No Results </html>"
    }

    val rb1 = new BoxPanel(Orientation.NoOrientation){
      contents+=indexTextlabel
    }


    val rb2 = new BoxPanel(Orientation.Horizontal){
      contents+= indexload
      contents+= indexloadlabel
    }
    rb2.maximumSize = x
    rb2.preferredSize = x
    rb2.minimumSize =x

    val rb3 = new BoxPanel(Orientation.Horizontal){
      contents+=indexjoin
      contents+=indexjoinlabel
    }

    rb3.maximumSize = x
    rb3.preferredSize = x
    rb3.minimumSize =x

    val bottomcontents = new BoxPanel(Orientation.Vertical){
      contents+=rb1
      contents+=rb2
      contents+=rb3
    }

    val sx =new Dimension(500,20)
    val middlecontents = new Separator()
    middlecontents.maximumSize = sx
    middlecontents.preferredSize = sx
    middlecontents.minimumSize =sx

    contents = new BoxPanel(Orientation.Vertical){
      contents+=topcontents
      contents+=middlecontents
      contents+=bottomcontents
    }

    listenTo(nativeload)
    listenTo(nativejoin)
    listenTo(indexload)
    listenTo(indexjoin)

    var df = sparkSession.emptyDataFrame;
    var df1 = sparkSession.emptyDataFrame;
    val filenameleft = "ResearchSample2"
    val filenameright = "ResearchResponse2"
    val path = "D:\\Education\\Spark\\Testdata\\"
    val dformatter = java.text.NumberFormat.getIntegerInstance

    reactions += {
      case ButtonClicked(b) =>
        if( b.name.equals("nativeload")) { //load data with spark native
          var labeltext = "<html>"
          var timer = Calendar.getInstance.getTimeInMillis()

          df = sparkSession.read.option("header", true).format("csv").load(path+filenameleft+".csv").cache()
          df1 = sparkSession.read.option("header", true).format("csv").load(path+filenameright+".csv").cache()

          labeltext += "Row count (Left table)  &nbsp;&nbsp;:&nbsp;" + dformatter.format(df.count()) + "<br>"
          labeltext += "Row count (Right table)&nbsp;:&nbsp;" + dformatter.format(df1.count()) + "<br>"
          labeltext += "Load time         &nbsp;:&nbsp;" + (Calendar.getInstance.getTimeInMillis() - timer) +" ms "
          labeltext += "</html>"
          nativeloadlabel.text = labeltext
        }else if ( b.name.equals("nativejoin")) {
          var labeltext = "<html>"
          var timer = Calendar.getInstance.getTimeInMillis()
          val lviewname = filenameleft+"native"
          val rviewname = filenameright+"native"
          df.createOrReplaceTempView(lviewname)
          df1.createOrReplaceTempView(rviewname)
          val r =  sparkSession.sql("select r.UserId,r.value from "+lviewname+" s join "+rviewname+" r on" +
            " s.UserId = r.UserId order by r.UserId")
          r.show()
          labeltext += "<font color='red'>  Execution time Join     : &nbsp;" + (Calendar.getInstance.getTimeInMillis() - timer) +" ms </font>"
          labeltext += "</html>"
          nativejoinlabel.text = labeltext
        } else if ( b.name.equals("indexload")) {
          val index_columns = "UserId,ProjectId" //Index columns and primary key (pk should be initial one)
          val source_type = IndexedFrameType.Csv //Source type
          val all_columns_sample = "Id,UserId,ProjectId" // this is to define schema for JSON
          val all_columns_response = "Id,UserId,ProjectId,Value" // this is to define schema for JSON

          var labeltext = "<html>"
          var timer = Calendar.getInstance.getTimeInMillis()

          val ldf = sparkSession.read.format("index")
            .option("IndexColumn",index_columns)
            .option("SourceFormat",source_type.toString)
            .option("schema",all_columns_sample)
            .load(path+filenameleft+".csv")

          val rdf = sparkSession.read.format("index")
            .option("IndexColumn",index_columns)
            .option("SourceFormat",source_type.toString)
            .option("schema",all_columns_response)
            .load(path+filenameright+".csv")

          labeltext += "Row count (Left table) &nbsp;&nbsp;&nbsp;:&nbsp;" + dformatter.format(ldf.count()) + "<br>"
          labeltext += "Row count (Right table) &nbsp;:&nbsp;" + dformatter.format(rdf.count()) + "<br>"
          labeltext += "Load time          &nbsp;:&nbsp;" + (Calendar.getInstance.getTimeInMillis() - timer) +" ms "
          labeltext += "</html>"
          indexloadlabel.text = labeltext
        }else if ( b.name.equals("indexjoin")) {
          var labeltext = "<html>"
          var timer = Calendar.getInstance.getTimeInMillis()
          val r =  sparkSession.sql("select r.UserId,r.value from "+filenameleft+" s join "+filenameright+" r on" +
            " s.UserId = r.UserId order by r.UserId")
          r.show()
          labeltext += "<font color='green'> Execution time Join     : &nbsp;" + (Calendar.getInstance.getTimeInMillis() - timer) +" ms </font>"
          labeltext += "</html>"
          indexjoinlabel.text = labeltext
        }

    }

    //contents = Button("click")(println("button"))
    size = new Dimension(500,400)
    centerOnScreen()
  }

  frame.visible = true
}
