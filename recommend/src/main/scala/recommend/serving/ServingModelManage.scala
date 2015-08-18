package recommend.serving


import akka.util.Timeout
import org.apache.spark.SparkContext
import recommend.RecomConfigure
import recommend.model.{ALSSpeedModel, ALSBatchModel}
import recommend.serving.schedule.ReturnOption

import recommend.util._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
 * Created by wuyukai on 15/7/2.
 */


case class RecommendItemMessage(userId: Int, page: Int, perPage:Int)
case class RecommendUserMessage(itemId: Int, page: Int, perPage:Int)
case class ReturnMessage(id:Int, list: Array[Int], option:ReturnOption.Value, errorReason: String = "")


class ServingModelManage(sc: SparkContext,conf: RecomConfigure){

  private var batchModel:ALSBatchModel =_

  private var speedModel:ALSSpeedModel =_


  implicit val global = scala.concurrent.ExecutionContext.global
  implicit val timeout = Timeout(3 seconds)


  val modelSavePath = conf.getOption("kaka.batch.model.path")


  //读取最近一次生成的Model
  if(modelSavePath != None) {
    println("initial Serving model Manage --> load begin last model")
    val path = recommend.util.HadoopIO.getLastModelPath(modelSavePath.get)
    println("initial Serving model Manage --> get last model path:" + path)
    updateBatchModel(new BatchUpdateMessage(path))
    println("initial Serving model Manage --> initial model is finished ")
  }




  /**
   * ALSSpeedModel 更新
   * @param msg: 更新消息
   * */
  def updateSpeedModel(msg: SpeedUpdateMessage): this.type = {
    if(speedModel != null) {
      val updataType = msg.table
      val option = msg.option
      //Thread
      Future{
        (msg.table, msg.option) match {
          case ("User", "Update") => speedModel.setUserFeature(msg.id, msg.feature)
          case ("User", "Remove") => speedModel.removeUserById(msg.id)
          case ("Item", "Update") => speedModel.setItemFeature(msg.id, msg.feature)
          case ("Item", "Remove") => speedModel.removeItemById(msg.id)
          case _ => println("can't resolve " + msg.table + "," + msg.option)
        }
      }
    }else {
      if(msg.option == "Update") {
        speedModel = new ALSSpeedModel(msg.feature.length)
        //Thread
        Future{
          msg.table match {
            case "User" => speedModel.setUserFeature(msg.id, msg.feature)
            case "Item" => speedModel.setItemFeature(msg.id, msg.feature)
            case _ => println("not this table: " + msg.table)
          }
        }
      }
    }
    this
  }

  /**
   * ALSBatchModel 更新
   * @param msg: 更新消息
   **/
  def updateBatchModel(msg: BatchUpdateMessage): this.type = {
    val modelPath = msg.path
    val newBathModel = try {
      val newModel = new ALSBatchModel
      newModel.loadModel(sc, modelPath)
    } catch {
      case e:Exception => return this
    }
    if(batchModel == null || speedModel == null) batchModel = newBathModel
    else {
      //Thread
      Future{
        val userIds = speedModel.getAllUserId()
        userIds.foreach{
          id => {
            val isExist = batchModel.lookupUser(id.toInt)
            if(isExist) speedModel.removeUserById(id)
          }
        }
      }
      //Thread
      Future{
        val itemIds = speedModel.getAllItemId()
        itemIds.foreach{
          id => {
            val isExist = batchModel.lookupItem((id.toInt))
            if(isExist) speedModel.removeItemById(id)
          }
        }

      }
    }
    this
  }



  def recommendItemForUser(userId: Int, itemNum: Int = 1000): Array[Int] = {
    synchronized{
      if(batchModel != null && speedModel != null){
        var userFeature = batchModel.lookupUserAndGet(userId.toInt)
        if(userFeature.isEmpty) {
          userFeature = speedModel.getUserFeatures(userId)
        }

        println("the Speed Feature Length is: "+ userFeature.length + "Speed Feature is " + userFeature.mkString("|"))
        //Thread
        if(!userFeature.isEmpty){
          val itemSpeed = Future{
            speedModel.recommendItemByFeatures(userFeature,itemNum)
          }
          val itemBatch = Future{
            batchModel.recommendItemByFeatures(userFeature).map(value => (value._1.toString, value._2))
          }

          val itemlist = for{
            sp <- itemSpeed.mapTo[Array[(Int, Double)]]
            ba <- itemBatch.mapTo[Array[(Int, Double)]]
          } yield {
              val item = sp ++ ba
              item.sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)
            }

          Await.result(itemlist, timeout.duration)

        } else Array.empty[Int]

      }else if(batchModel != null && speedModel == null) {


        val itemlist = batchModel.recommendItemById(userId).map(value => (value._1, value._2))
        itemlist.sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)

      }else if(batchModel == null && speedModel != null) {

         speedModel.recommendItemById(userId)
           .sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)

      } else Array.empty[Int]
    }

  }

  def recommendUserForItem(itemId: Int, UserNum: Int = 1000):Array[Int] = {
    synchronized{
      if(batchModel != null && speedModel != null){
        var itemFeature = batchModel.lookupItemAndGet(itemId.toInt)

        if(itemFeature.isEmpty) itemFeature = speedModel.getItemFeatures(itemId)

        if(!itemFeature.isEmpty){
          val userlist = speedModel.recommendUserByFeatures(itemFeature, UserNum) ++
            batchModel.recommendUserByFeatures(itemFeature).map(value => (value._1, value._2))
          userlist.sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)
        } else Array.empty[Int]

      }else if(batchModel != null && speedModel == null) {
        val itemFeature = batchModel.lookupItemAndGet(itemId.toInt)
         Array.empty[Int]
        if(!itemFeature.isEmpty){
          val userlist = batchModel.recommendUserByFeatures(itemFeature, UserNum).map(value => (value._1, value._2))
          userlist.sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)
        } else Array.empty[Int]

      }else if(batchModel == null && speedModel != null) {
        val itemFeature =  speedModel.getItemFeatures(itemId)

        if(!itemFeature.isEmpty){
          val userlist = speedModel.recommendUserByFeatures(itemFeature, UserNum)
          userlist.sortWith{case((id1,rating1),(id2,rating2)) => rating1 > rating2}.map(_._1)
        } else Array.empty[Int]
      }else Array.empty[Int]
    }
  }


  def printlnState(): Unit = {
    println("ModelManageState: \n")
    if (batchModel != null) {
      println("BatchModel is not null\n")
    }
    else println("BatchModel is null\n")
    if (speedModel != null) {
      println("SpeedModel is not null\n")
      val userNum = speedModel.getAllUserId().size
      println("UserNum: " + userNum)
      val itemNum = speedModel.getAllItemId().size
      println("ItemNum: " + itemNum)
    }
    else println("SpeedModel is null\n")
  }

}

