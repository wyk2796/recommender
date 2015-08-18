package recommend.serving.cache

import recommend.serving.{ReturnMessage, RecommendItemMessage}
import recommend.serving.schedule.ReturnOption._

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

import scala.concurrent.Future

/**
 * Created by Yukai on 2015/7/16.
 */

case class Item(userId:Int, beginTime:Long, recommendList:Array[Int], offset:Int)

trait ListCache {

  protected val CACHEMAX = 1000000

  protected val TIMEOUT = 1200000

  protected val HEADSPACE = CACHEMAX * 0.1



}

class RecommendUserListCache extends ListCache{

  private var listCache = new HashMap[Int ,Item]()

  implicit val global = scala.concurrent.ExecutionContext.global

  var cleanNumMax = 2

  def isFull:Boolean = listCache.size == CACHEMAX

  def length = listCache.size

  def updateList(id:Int,recommendList:Array[Int], offset:Int):Unit ={
    listCache = listCache + ((id, new Item(id, System.currentTimeMillis() ,recommendList, offset)))
  }

  /**
   * 插入或修改新数据
   * @param id:用户id, recommendList:用户推荐列表
   * @return 是否插入成功
   * */
  def setNewList(id:Int,recommendList:Array[Int]):Boolean = {
    if(CACHEMAX - length < HEADSPACE && cleanNumMax > 0){
      cleanNumMax -= 1
      Future{
        clean()
      }
      cleanNumMax += 1
    }

    if(length < CACHEMAX || listCache.contains(id)){
      updateList(id,recommendList,0)
      true
    } else false

  }


  def delete(id:Int): this.type ={
    listCache = listCache - id
    this
  }

  def pushRecommendList(msg:RecommendItemMessage):ReturnMessage = {

    val (list, offset) = try{
      val item = listCache(msg.userId)
      (item.recommendList,item.offset)
    } catch{
      case e:NoSuchElementException => {
        return new ReturnMessage(msg.userId, Array.empty[Int], CacheFindFail)
      }
    }



    val end = offset + msg.perPage

    Future{
      updateList(msg.userId,list, math.min(end, list.length))
    }

    val recommendList = for(i <- offset until end) yield {
      if(i < list.length) list(i)
      else list(i % list.length)
    }

    println("the offset is " + offset)
    println("the end is " + end)
    println("the list length is " + list.length)
    if(list.length - end < 200) new ReturnMessage(msg.userId, recommendList.toArray, UpdateCache)
    else new ReturnMessage(msg.userId,recommendList.toArray, ReturnList)

  }

  /**
   * 缓存空间清理
   * */
  def clean(): Unit ={
    val deleteList = listCache.filter{
      case(id ,item) => {
        System.currentTimeMillis() - item.beginTime > TIMEOUT
      }
    }
    deleteList.foreach(value => delete(value._1))
    if(CACHEMAX - listCache.size < HEADSPACE){
      val tmp = ArrayBuffer[(Int, Long)]()
      var changerFlag = true
      listCache.foreach{
        case(id, item) => {
          val stayTime = System.currentTimeMillis() - item.beginTime
          if(tmp.length < HEADSPACE) tmp += ((id, stayTime))
          else {
            var longer = tmp(0)
            if(changerFlag){
              tmp.foreach{
                value => {
                  if(value._2 < longer._2) longer = value
                }
              }
              changerFlag = false
            }
            if(stayTime > longer._2) {
              tmp -= longer
              tmp += ((id, stayTime))
              changerFlag = true
            }
          }
        }
      }
      tmp.foreach(elem => delete(elem._1))
    }
  }
}


