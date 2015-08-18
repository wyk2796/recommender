package recommend.serving

import akka.util.Timeout
import recommend.RecomConfigure
import recommend.serving.cache.RecommendUserListCache
import recommend.serving.schedule.RecommendActor
import scala.actors.Actor
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import scala.concurrent.duration._
import scala.language.postfixOps
/**
 * Created by Yukai on 2015/7/9.
 */
class TestRecommendActor(conf:RecomConfigure,
                         modelManage: ServingModelManage,
                         dataCache: RecommendUserListCache) extends Actor {

  val testInterval = conf.get("kaka.test.recommend.iterval").toInt
  implicit val timeout = Timeout(3 seconds)
  implicit val global = scala.concurrent.ExecutionContext.Implicits.global



  def act() = Actor.loop{

    val useId = (math.random * 20000000).toInt
    val page = (math.random * 100).toInt
    val prePage = (math.random * 30).toInt
    val msg = new RecommendItemMessage(useId,page,prePage)
    val t1 = System.currentTimeMillis()

    val testSys = ActorSystem("TestSystem")
    val recomA= testSys.actorOf(Props(new RecommendActor(conf,dataCache)))
    implicit val global = scala.concurrent.ExecutionContext.Implicits.global
    println("send data to RecommendActor")
    val resp = ask(recomA, msg)
    println("receive the result in test")
//    val result = resp.asInstanceOf[ReturnMessage]
//    println("UserId: " + result.id + " receive Recommend Item List is :" + result.list.mkString(","))
//    react {
//      case ReturnMessage(id,list,status) => {
//        val t2 = System.currentTimeMillis()
//        if(!list.isEmpty){
//          println("Recommend Cost Time is" + (t2 - t1))
//          println("UserId: " + id + " receive Recommend Item List is :" + list.mkString(","))
//        }
//      }
//      case _ => {
//        println("TestActor receive wrong message!")
//        (useId,Array.empty[String])
//      }
//    }


    resp.foreach{
      value =>{
        val list = value match {
          case ReturnMessage(id,list,status,errorReason) => (id, list)
          case _ => {
            println("TestActor receive wrong message!")
            (useId,Array.empty[String])
          }
        }
        val t2 = System.currentTimeMillis()
        if(!list._2.isEmpty){
          println("Recommend Cost Time is" + (t2 - t1))
          println("UserId: " + list._1 + " receive Recommend Item List is :" + list._2.mkString(","))
        }else println("receive the list is empty")
      }
    }
    Thread.sleep(500)
  }
}
