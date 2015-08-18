package recommend.serving.candidate

import akka.actor.{Actor,ActorLogging}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.hadoop.hbase.client.{Get, Put}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import recommend.RecomConfigure
import recommend.database.HbaseContext
import recommend.database.HbaseContext._
import recommend.util.BaseString._
import recommend.util.KafkaString._
import recommend.serving.candidate.PullType._
import recommend.serving.cache.RecommendUserListCache


/**
 * Created by wuyukai on 15/7/31.
 */
class ItemFamily(conf:RecomConfigure ,dataCache: RecommendUserListCache) extends Actor with ActorLogging{


  private val candidateActor = context.actorSelection("../CandidateMange")

  implicit val gc = scala.concurrent.ExecutionContext.global

  implicit val timeout = Timeout(2 seconds)

  private val zkIp = conf.get(Prifix + ZookeeperConnect)

  private val recommendHistory = new HbaseContext(zkIp,"RecommendHistory")

  private val recommendDatabase = new HbaseContext(zkIp,"RecommendDatabase")


  override def preStart: Unit ={

    if(!recommendHistory.isExist()) {
      val columns = Array("history_1","history_2")
      if(recommendHistory.createTable(columns)) log.info("ItemFamily: Table RecommendHistory create successful")
    }

    if(!recommendDatabase.isExist()) {
      val columns = Array("recommend_params")
      if(recommendDatabase.createTable(columns)) log.info("ItemFamily: Table RecommendDatabase create successful")
    }

  }


  /**
   * 排序混合策略, 以后扩展算法点
   * @param list: 物品单元集合
   * @return 排序融合后得集合, item id 和 候选集id数组
   * */
  def itemTogether(list:Array[ItemUnit]): List[(Int, ArrayBuffer[Int])] = {

    if(list.isEmpty) return List.empty[(Int, ArrayBuffer[Int])]

    val itemSet = HashMap[Int,ArrayBuffer[Int]]()
    list.foreach{
      item =>
        if(itemSet.contains(item.itemId)) itemSet(item.itemId) += item.candidateListId
        else itemSet(item.itemId) = ArrayBuffer(item.candidateListId)
    }

//    val res = itemSet.toArray.sortWith{
//      case((id1, arr1),(id2, arr2)) => {
//        val rate1 = arr1.length * 100 + math.random * 100
//        val rate2 = arr2.length * 100 + math.random * 100
//        rate1 > rate2
//      }
//    }
    val res = sort(itemSet.toArray)

    log.info(res.map(line => line._1 + line._2.mkString(",")).mkString("|"))
    res.toList
  }

  //临时加插入排序算法
  def sort(arr:Array[(Int,ArrayBuffer[Int])]):Array[(Int,ArrayBuffer[Int])] = {

    val arrTemp = arr.map{
      case(id, arr) => (id, arr , arr.length * 100 + math.random * 100)
    }
    val result = ArrayBuffer[(Int, ArrayBuffer[Int])]()

    for(i <- 0 until arrTemp.length){
      var id = 0
      var rate = 0.0
      for(i<- 0 until arrTemp.length){
        if(rate < arrTemp(i)._3) {
          rate = arrTemp(i)._3
          id = i
        }
      }
      result += ((arrTemp(id)._1, arrTemp(id)._2))
      arrTemp(id) = (arrTemp(id)._1,arrTemp(id)._2, 0.0)
    }
    result.toArray
  }



  /**
   * 存储推荐历史记录
   * 保存推荐item id 和推荐源 id
   * */
  def saveRecommendHistory(userid:Int, list:List[(Int, ArrayBuffer[Int])]) : Unit = {

    if(list.isEmpty) return

    val table = recommendHistory.getExistTable()

    val get = new Get(userid)
    val h1Set = HashMap[Int, String]()
    val h2Set = HashMap[Int, String]()
    if(table.exists(get)){
      val result = table.get(get)
      val h1Map = result.getFamilyMap("history_1")

      val keys1 = h1Map.keySet().iterator()
      while(keys1.hasNext){
        val key = keys1.next()
        h1Set(key) = h1Map.ceilingKey(key)
      }

      val h2Map = result.getFamilyMap("history_2")

      val keys2 = h2Map.keySet().iterator()
      while(keys2.hasNext){
        val key = keys2.next()
        h2Set(key) = h2Map.ceilingKey(key)
      }

    }


    val put = new Put(userid)

    for((id, arr) <- list) {
      for(candId <- arr){
        if(h1Set.contains(candId)){
          val h1list = h1Set(candId)
          h1Set(candId) = h1list + "," + id
        } else h1Set(candId) = id.toString
        if(h2Set.contains(candId)){
          val h2list = h2Set(candId)
          h2Set(candId) = h2list + "," +id
        } else h2Set(candId) = id.toString
      }
    }

    h1Set.foreach{
      case(id, value) => {
        put.addColumn("history_1",id,value)
      }
    }
    h2Set.foreach{
      case(id, value) => {
        put.addColumn("history_1",id,value)
      }
    }

    table.put(put)
    table.close()
  }

  def pushListToCache(userid:Int, list: List[(Int, ArrayBuffer[Int])]): Unit = {

    val ids = list.map(_._1).toArray

    if(dataCache.setNewList(userid, ids)) log.info(s"User_${userid} RecommendList save to Cache successful")
    else log.info(s"User_${userid} RecommendList save to Cache fail")


  }

  def pushListToDataBase(userid:Int, list: List[(Int, ArrayBuffer[Int])]): Unit = {

    val table = recommendDatabase.getExistTable()
    val put = new Put(userid)
    val rList = list.map(_._1).mkString(",")

    put.addColumn("recommend_params", "recommend_list", rList)
    put.addColumn("recommend_params", "update_time", System.currentTimeMillis())
    put.addColumn("recommend_params", "offset", 0)

    table.put(put)
    table.close()

  }


  def receive = {

    case msg:PullListMessage => {
      val pullType = msg.pullType
      log.info("itemFamily receive message")
      val result = ask(candidateActor, msg)
      for(elem <- result.mapTo[Array[ItemUnit]]){

        val items = itemTogether(elem)
        pullType match{
          case ReturnDirect => sender ! items.map(_._1).toArray
            pushListToCache(msg.userid, items)
            pushListToDataBase(msg.userid, items)

          case ReturnToDataBase =>
            pushListToCache(msg.userid,items)
            pushListToDataBase(msg.userid, items)
        }
        saveRecommendHistory(msg.userid,items)
      }
    }

    case _ => log.error("ItemFamily: receive wrong message")
  }

}
