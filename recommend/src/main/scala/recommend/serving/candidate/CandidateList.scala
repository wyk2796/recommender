package recommend.serving.candidate

import akka.actor.{ActorLogging, Actor}
import recommend.serving.candidate.PullType.PullType

//import recommend.serving.schedule.ReturnOption.ReturnStatus


/**
 * Created by apple on 15/7/31.
 */

case class ItemUnit(itemId:Int, candidateListId:Int)

sealed trait CandidateMessage

case class PullListMessage(userid:Int, itemNum:Int, pullType:PullType.Value)

case class CandidateModelMessage(id:Int, num:Int, offset:String) extends CandidateMessage
case class CandidateListMessage(num:Int, offset:String) extends CandidateMessage
case class CandidateReturnMessage(Cid:Int, list:Array[ItemUnit], offset:String, statue:ReturnStatus.Value) extends CandidateMessage

case class CreateCandidateMessage(candidateType:String, storeType:String, url:String, tableName:String, algo:String)
case class DeleteCandidateMessage(candidateId:Int)

trait CandidateList extends Actor with ActorLogging{



}


