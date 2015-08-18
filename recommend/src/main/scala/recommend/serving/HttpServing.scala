package recommend.serving


import org.json4s.JsonAST.JObject
import recommend.RecomConfigure
import recommend.serving.cache.RecommendUserListCache
import recommend.serving.schedule.{ReturnOption, RecommendActor}
import spray.routing.HttpService
import akka.actor._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import akka.util.Timeout
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.io.IO
import spray.can.Http
import akka.pattern.ask
import scala.language.postfixOps
import recommend.util.BaseString._

/**
 * Created by wuyukai on 15/7/2.
 */


class HttpServing(conf: RecomConfigure,
                  dataCache:RecommendUserListCache) extends Actor with HttpService with ActorLogging{

  def actorRefFactory = context

  def receive = runRoute(myRoute)

  implicit val timeout = Timeout(3 seconds)

  val recomA = context.actorOf(Props(new RecommendActor(conf, dataCache)))

  val parameterDeal = parameterMap {
    value => {
      val id = value.get("user_id")
      val page = value.get("page")
      val perPage = value.get("per_page")


      log.info("begin to recommend")
      val list = if (id != None && page != None && perPage != None) {
        val msg = new RecommendItemMessage(id.get.toInt, page.get.toInt, perPage.get.toInt)
        log.info("receive the message is:" + id.get +"|"+ page.get +"|"+ perPage.get)
         doRequest(msg,recomA)

      } else if(id != None && (page == None || perPage == None)) {
        val msg = new RecommendItemMessage(id.get.toInt, 1, 10)
        log.info("receive the message is:" + id.get +"|"+ "None" +"|"+ "None")
        doRequest(msg,recomA)
      } else (-1 ,Array.empty[Int])

      val json = if (list._1 == -1) {
        toJson("null", "null", 1, "ID is null")
      } else if (list._2.isEmpty) {
        toJson(list._1.toString, "null", 1 ,"This id is not in model")
      } else {
        toJson(list._1.toString, list._2.mkString(",") ,0 ,"")
      }
      val jsReturn = compact(render(json))

      complete {
        jsReturn
      }

    }
  }

  def toJson(id:String, list:String, errorCode:Int = 0, errorReason: String):JObject = {
    pair2jvalue("user_id" -> id) ~ pair2jvalue("video_ids" -> list) ~ pair2jvalue("error_code" -> errorCode) ~ pair2jvalue("error_reason" -> errorReason)
  }

  def doRequest(msg: RecommendItemMessage, recomA: ActorRef): (Int, Array[Int]) = {
    val result = recomA ? msg
    try{
      val reposon = Await.result(result, timeout.duration)
      reposon match {
        case result: ReturnMessage => {
          result.option match {
            case ReturnOption.Error => {
              log.error(result.errorReason)
              (result.id, result.list)
            }
            case ReturnOption.ReturnList | ReturnOption.UpdateCache =>{
              log.info(result.id +  " : Recommend Successful")
              (result.id, result.list)
            }
            case _ => log.error("receive wrong message!")
                      (msg.userId, Array.empty[Int])
          }

        }
        case _ => {
          log.info("receive wrong message!")
          (msg.userId, Array.empty[Int])
        }
      }
    } catch{
      case ex:Exception => log.info("DoRequest create a Problem")
        (-1 ,Array.empty[Int])
    }
  }

  val myRoute = path("api" / "video"/ "recommend"){
    get{
      parameterDeal
    } ~
    post{
      parameterDeal
    }
  }
}

class BootServing(conf:RecomConfigure, serviceSys:ActorSystem) {

  val paramTimeOut = conf.getInt(Prifix + ServingTimeout, 5)
  val paramThreadNum = conf.getInt(Prifix + ServingThreadNum, 10)
  val paramIp = conf.get(Prifix + ServingIp,"localhost")
  val paramPort = conf.getInt(Prifix + ServingPort, 40000)


  implicit val timeout = Timeout(paramTimeOut.seconds)
  implicit val global = scala.concurrent.ExecutionContext.Implicits.global
  implicit val actsys = serviceSys

  def createHttpServing(dataCache: RecommendUserListCache): Unit = {
    for(portTmp <- paramPort until paramPort + paramThreadNum){
      val service = serviceSys.actorOf(Props(new HttpServing(conf, dataCache)), s"recommend-service-${portTmp}")
     IO(Http) ? Http.Bind(service, interface = paramIp, port = portTmp) onFailure {
       case e:Exception => serviceSys.shutdown()
     }
    }
  }

}
