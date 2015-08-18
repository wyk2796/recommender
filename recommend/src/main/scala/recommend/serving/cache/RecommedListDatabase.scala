package recommend.serving.cache

import org.apache.hadoop.hbase.client.{Put, Get}
import recommend.RecomConfigure
import recommend.database.HbaseContext
import recommend.database.HbaseContext._
import recommend.serving.ReturnMessage
import recommend.util.BaseString._
import recommend.util.KafkaString._
import recommend.serving.schedule.ReturnOption._



/**
 * Created by wuyukai on 15/8/4.
 */
class RecommedListDatabase(conf:RecomConfigure){

  private val zkIp = conf.get(Prifix + ZookeeperConnect)

  val recommendTable = new HbaseContext(zkIp, "RecommendDatabase")


  def recommendItem(userId:Int, num :Int): ReturnMessage = {
    val table = recommendTable.getExistTable()
    val get = new Get(userId)

    get.addColumn("recommend_params","recommend_list")
    get.addColumn("recommend_params","offset")

    val (recommendStr:String, offset:Int) = try{
      val result = table.get(get)
      val list:String = result.getValue("recommend_params","recommend_list")
      val offset:Int = result.getValue("recommend_params","offset")
      (list, offset)
    } catch{
      case e:Exception => return new ReturnMessage(userId, Array.empty[Int], DatabaseFindFail)
    }


    val recommendList = recommendStr.split(",")

    val end = offset + num

    val put  = new Put(userId)

    put.addColumn("recommend_params", "offset", end % recommendList.length)

    table.put(put)

    table.close()

    val relist = for(i <- offset until end) yield {
      if(i < recommendList.length) recommendList(i).toInt
      else recommendList(i % recommendList.length).toInt
    }

    if(recommendList.length - (offset + num) < 50) new ReturnMessage(userId, relist.toArray, UpdateDatabase)
    else new ReturnMessage(userId, relist.toArray, WriterToCache)
  }


  def getRecommendList(userId: Int): Array[Int] = {
    val table = recommendTable.getExistTable()
    val get = new Get(userId)
    get.addColumn("recommend_params","recommend_list")
    val result = table.get(get)

    val recommendStr:String = result.getValue("recommend_params","recommend_list")

    recommendStr.split(",").map(_.toInt)
  }



}
