package recommend.serving.candidate

import org.apache.hadoop.hbase.client.{Result, Get, HTable}
import recommend.database.HbaseContext._
import recommend.serving.candidate.ReturnStatus._


/**
 * Created by apple on 15/7/31.
 */
abstract class ModelCandidateList(table:HTable, id:Int) extends CandidateList{

  val tableName = table.getName.getNameAsString

  //TODO offset problem
  protected def getCandidateList(userId:String, num:Int,offset:Int):CandidateReturnMessage

  def receive = {
    case CandidateModelMessage(id, num, offset) => sender ! getCandidateList(id.toString, num, offset.toInt)
    case _ => sender ! new CandidateReturnMessage(id,Array.empty[ItemUnit], "", Error)
      log.error(s"ALSCHbaseList ${id} : receive wrong message.")
  }

}

class ALSCandidateList(table: HTable, id: Int) extends ModelCandidateList(table, id){

  val familyName = "video_ids"

  val columnName = "ids"






  def getCandidateList(userId:String, num:Int,offset:Int):CandidateReturnMessage = {

    val get = new Get(userId)

    get.addColumn(familyName,columnName)

    val list:String = try{
      val result = table.get(get)
      result.getValue(familyName, columnName)
    } catch{
      case e:Exception => log.error(s"ALSCHbaseList ${id} : get data from table ${table.getTableName()} create an error: " +
        e.getStackTraceString)
        return new CandidateReturnMessage(id, Array.empty[ItemUnit], offset.toString, Error)
    }finally{
      table.close()
    }
    val clist = list.split(",").map(videoId => new ItemUnit(videoId.toInt, id)).take(num)

    new CandidateReturnMessage(id, clist, offset.toString, Successful)
  }


}
