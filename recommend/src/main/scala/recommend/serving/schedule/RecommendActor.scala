package recommend.serving.schedule

import ReturnOption._
import recommend.serving.candidate.{CandidateManage, ItemFamily, PullListMessage}
import recommend.{RecomConfigure, CacheException, ALSRecommendException}
import recommend.serving.cache.RecommendUserListCache
import recommend.serving.cache.RecommedListDatabase
import recommend.serving.{RecommendUserMessage, RecommendItemMessage, ReturnMessage, ServingModelManage}
import recommend.serving.candidate.PullType._

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout


import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * Created by Wuyukai on 2015/7/17.
 */
class RecommendActor (conf: RecomConfigure,
                      dataCache: RecommendUserListCache) extends Actor with ActorLogging {

  //当前正在处理的请求
  private var recommendMessage:RecommendItemMessage =_

  private var masterSender:ActorRef =_
  private var status = ActorStatus.Available
  implicit val global = scala.concurrent.ExecutionContext.Implicits.global

  implicit val timeout = Timeout(3 seconds)

  val recomDatabase = new RecommedListDatabase(conf)

  override def preStart: Unit = {
    context.actorOf(Props(new CacheActor(dataCache)), "CacheActor")
    context.actorOf(Props(new ModelActor(recomDatabase)), "ModelActor")
    context.actorOf(Props(new ItemFamily(conf,dataCache)), "ItemFamily")
    context.actorOf(Props(new CandidateManage(conf)), "CandidateMange")
  }

  def receive = {
    case msg:RecommendItemMessage if status == ActorStatus.Available => {

      status = ActorStatus.Busying

      context.actorSelection("CacheActor") ! msg
      context.actorSelection("ModelActor") ! msg

      masterSender = sender
      recommendMessage = msg

    }
    case msg:ReturnMessage if status == ActorStatus.Busying => {
      msg.option match {
        case ReturnList => {

          masterSender ! msg

          status = ActorStatus.Available
          log.info("return channal is returnList")
        }
        case UpdateCache => {

          masterSender ! msg

          log.info("return channal is UpdateCache")
          Future{
            val list = recomDatabase.getRecommendList(msg.id)
            dataCache.setNewList(msg.id, list)
          }
          status = ActorStatus.Available
        }
        case WriterToCache => {
          log.info("return channel is WriterToCache")
          val recommendMsg = if (!msg.list.isEmpty) {

            val recommendList = for (i <- 0 until math.min(recommendMessage.perPage,msg.list.length))
              yield msg.list(i)
            new ReturnMessage(msg.id, recommendList.toArray, ReturnList)
          } else ReturnMessage(msg.id, Array.empty[Int], Error, "This id is not in model")

          masterSender ! recommendMsg

          Future{
            log.info("set offset : " + math.min(recommendMessage.perPage,msg.list.length))
            dataCache.updateList(msg.id, msg.list, math.min(recommendMessage.perPage,msg.list.length))
          }

          status = ActorStatus.Available

        }
        case CacheFindFail => log.info("return channel is FindFail, ready receive database data")

        case UpdateDatabase => {

          log.info("return channel is UpdateDatabase, ready receive database data")

          masterSender ! msg

          context.actorSelection("ItemFamily") ! new PullListMessage(msg.id, 2000, ReturnToDataBase)

        }

        case DatabaseFindFail => {

          log.info("return channel is DatabaseFindFail, ready receive Candidate data")

          val result = ask(context.actorSelection("ItemFamily"), new PullListMessage(msg.id, 2000, ReturnToDataBase))

          result.onFailure{
            case e:Exception => log.error("RecommendActor : DatabaseFindFail : create an Error:" + e.getStackTraceString)
              masterSender ! new ReturnMessage(msg.id, Array.empty[Int], Error)
          }

          val num = recommendMessage.perPage

          for(elem <- result.mapTo[Array[Int]]){
            masterSender ! new ReturnMessage(msg.id,elem.take(num), ReturnList)
            dataCache.updateList(msg.id, elem, num)
          }

        }


        case Error => masterSender ! msg
          log.error("An error encounter")

        case _ => {
          masterSender ! new ReturnMessage(msg.id, Array.empty[Int], Error,"option receive the wrong message")
          status = ActorStatus.Available
        }
      }
    }

    case "status" => sender ! status

    case _ if status == ActorStatus.Available => log.info("abandon message because result is returned! ")

    case _ => {
      masterSender ! new ReturnMessage(-1 , Array.empty[Int], Error, "RecommendActor receive the wrong message")
      status = ActorStatus.Available
    }



  }


}


class CacheActor(dataCache: RecommendUserListCache) extends Actor with ActorLogging{

  def receive = {

    case msg:RecommendItemMessage => {
      try{
        sender ! dataCache.pushRecommendList(msg)
      }catch {
        case e: Exception => {
          log.error(e.getMessage + ":" + e.getStackTraceString)
          sender ! new ReturnMessage(-1, Array.empty[Int], Error)
        }
      }
    }
    case _ => {
      log.info("CacheActor receive the wrong message")
      sender ! new ReturnMessage(-1, Array.empty[Int], Error)
    }

  }


}

class ModelActor(RecomDatabase:RecommedListDatabase) extends Actor with ActorLogging{

  def receive = {

    case RecommendItemMessage(userId, page, perPage) => {
      try {

        val list = RecomDatabase.recommendItem(userId, perPage)
        sender ! list

      } catch {

        case e: Exception => {
          log.error(e.getMessage + e.getStackTraceString)
          sender ! new ReturnMessage(userId, Array.empty[Int], Error, "RecommendDatabase create an error!")
        }
      }

    }


    case _ => {
      log.info("ModelActor receive the wrong message")
      sender ! new ReturnMessage(-1 , Array.empty[Int], Error)
    }

  }
}

