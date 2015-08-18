package recommend.serving.candidate

import org.apache.hadoop.hbase.client.{Result, Scan, HTable}
import recommend.database.HbaseContext._
import recommend.serving.candidate.ReturnStatus._

/**
 * Created by apple on 15/7/31.
 */
class TopCandidateList(table:HTable, id: Int) extends CandidateList{

  val familyName = "basic"

  val columnName = "video_id"

  val tableName:String = table.getTableName

  val rowKeyNums = 5


  def getCandidateList(num:Int ,offset: String):CandidateReturnMessage = {

    val scan = new Scan()
    scan.setStartRow(offset)
    val noffset = nextOffset(num, offset)
    scan.setStopRow(noffset)
    val resultScan = table.getScanner(scan)

    val clist = try{
      for(result <- resultScan.next(num)) yield{
        val video:String = result.getValue(familyName,columnName)
        new ItemUnit(video.toInt,id)
      }
    }catch{
      case e:Exception => log.error(s"TopCandidateList ${id} : get data from ${table.getTableName} create an error :" + e.getStackTraceString)
        return new CandidateReturnMessage(id, Array.empty[ItemUnit], offset , Successful)
    }finally {
      resultScan.close()
      table.close()
    }

    new CandidateReturnMessage(id, clist, offset , Successful)

  }

  def nextOffset(num:Int, offset:String): String = {

    var numStr = (num + offset.toInt).toString
    for(i <- 0 until rowKeyNums - numStr.length){
      numStr = "0" + numStr
    }
    numStr
  }

  def receive = {
    case CandidateListMessage(num, offset) => sender ! getCandidateList(num, offset)
    case _ => sender ! new CandidateReturnMessage(id, Array.empty[ItemUnit], "", Error)
      log.error(s"TopCandidateList ${id} : receive wrong message")

  }

}
