package recommend.speed


import akka.actor.ActorRef
import akka.util.Timeout
import org.apache.hadoop.hbase.client.{Get, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import recommend.RecomConfigure
import recommend.database.HbaseContext
import recommend.database.HbaseContext._
import recommend.model.ALSBatchModel
import recommend.util.BaseString._
import recommend.util.KafkaString._
import scala.concurrent.duration._



/**
 * Created by wuyukai on 15/6/30.
 */
class ALSSpeedModelManage(conf:RecomConfigure,kafkaSender:ActorRef) {

  private var batchModels:ALSBatchModel =_

  private var currStreamModel:ALSBatchModel =_

  implicit val ec = scala.concurrent.ExecutionContext.global

  implicit val timeout = Timeout(3 seconds)

  private val zkIp = conf.get(Prifix + ZookeeperConnect)

  private val recommendDatabase = new HbaseContext(zkIp,"ALSRecommender")

  if(!recommendDatabase.isExist()){
    recommendDatabase.createTable(Array("video_ids"))
    println("DataBase create is Ok")
  }

  def registerCandidate(): Unit = {

    val candidateName = recommendDatabase.tn.getNameAsString
    val candidateTable = new HbaseContext(zkIp, "CandidateListInformation")

    if(!candidateTable.isExist()){
      candidateTable.createTable(Array("candidate_information"))
      println("CandidateListInformation create is Ok")
    }

    val htable = candidateTable.getExistTable()

    val get = new Get(candidateName)

    if(!htable.exists(get)){
      val put = new Put(candidateName)
      put.addColumn("candidate_information", "store_type", "hbase")
      put.addColumn("candidate_information", "url", zkIp)
      put.addColumn("candidate_information", "candidate_list_type", "mode")
      put.addColumn("candidate_information", "algo", "als")
      htable.put(put)
    }

    htable.close()

  }






  def updateCurrStreamModel(newCurrStreamModel: ALSBatchModel): Unit = {
    synchronized{
      if(currStreamModel != null) currStreamModel.unpersist()
      currStreamModel = newCurrStreamModel
    }
  }

  def updateCurrrBatchModel(newBatchModel: ALSBatchModel): Unit = {
    synchronized{
      if(batchModels != null) batchModels.unpersist()
      batchModels = newBatchModel
      batchModels.persist()
    }
  }




  def recommendItemBatch(itemNum:Int = 5000): Unit = {
    synchronized{
      if(batchModels != null && currStreamModel != null){

        require(batchModels.rank == currStreamModel.rank, "batchModel 和 currStreamModel 特征维度必须一致。")

        val userFeature = currStreamModel.getUserFeature

        val recommendList1 = batchModels.recommendForAllUser(userFeature, itemNum)

        val recommendList2 = currStreamModel.recommendItemForUsers(math.min(currStreamModel.itemNum, itemNum).toInt)

        val list = recommendList1.union(recommendList2).reduceByKey(_++_).map(line =>(line._1, line._2.sortBy(_._2).reverse.take(itemNum)))

        ALSSpeedModelManage.saveToDatabase(list, recommendDatabase)

      }else if(batchModels == null && currStreamModel != null){

        val list = currStreamModel.recommendItemForUsers(math.min(currStreamModel.itemNum, itemNum).toInt)

        ALSSpeedModelManage.saveToDatabase(list, recommendDatabase)

      }else{

      }
    }
  }

  /**
   * 针对数据量小的流, 直接推荐 不生成Model
   * */
  def recommendItemLess(iter:Iterator[Int] , itemNum: Int = 5000): Unit = {

    synchronized{
      if(batchModels != null && currStreamModel != null){

        require(batchModels.rank == currStreamModel.rank, "batchModel 和 currStreamModel 特征维度必须一致。")

        val usersList= iter.map{
          userId =>{
            val recommendList1 = batchModels.recommendItemById(userId, itemNum)

            val recommendList2 = currStreamModel.recommendItemById(userId, itemNum)

            val list = (recommendList1 ++ recommendList2).sortWith((a,b) => a._2 > b._2).take(itemNum)

            (userId,list)
          }
        }

        ALSSpeedModelManage.saveToDatabase(usersList, recommendDatabase)


      }else if(batchModels == null && currStreamModel != null){

        val userList = iter.map{
          userId => {

            val recommendList = currStreamModel.recommendItemById(userId, itemNum)

            (userId, recommendList.take(itemNum))

          }
        }


        ALSSpeedModelManage.saveToDatabase(userList, recommendDatabase)

      }else if(batchModels != null && currStreamModel == null){

        val userList = iter.map{
          userId => {

            val recommendList = batchModels.recommendItemById(userId, itemNum)

            (userId, recommendList.take(itemNum))

          }
        }

        ALSSpeedModelManage.saveToDatabase(userList, recommendDatabase)
      }else {

      }

    }

  }




  def printlnState(): Unit = {
    println("ModelManageState: ")
    if (batchModels != null) {
      println("BatchModel is not null")
      println("UserNum: " + batchModels.userNum)
      println("ItemNum:" + batchModels.itemNum)
    }
    else println("BatchModel is null")
    if (currStreamModel != null) {
      println("SpeedModel is not null")
      val userNum = currStreamModel.userNum
      println("UserNum: " + userNum)
      val itemNum = currStreamModel.itemNum
      println("ItemNum: " + itemNum)
    }
    else println("SpeedModel is null")
  }

}


object ALSSpeedModelManage {

  def saveToDatabase(data:RDD[(Int,Array[(Int,Double)])], recommendDatabase :HbaseContext): Unit = {

    val hConf = recommendDatabase.hbaseConf
    val tableName = recommendDatabase.tn.getNameAsString

    val jc = new JobConf(hConf)

    jc.setOutputFormat(classOf[TableOutputFormat])

    jc.set(TableOutputFormat.OUTPUT_TABLE, tableName)


    val familyName = "video_ids"

    val columnName = "ids"

    println("tableName:   " + tableName)

    data.filter(!_._2.isEmpty).map{
      case(id,recommendlist) => {
        val put = new Put(id.toString)
        put.addColumn(familyName, columnName, recommendlist.map(_._1).mkString(","))
        (new ImmutableBytesWritable ,put)
       }
    }.saveAsHadoopDataset(jc)

  }


  def saveToDatabase(iter:Iterator[(Int, Array[(Int,Double)])], recommendDatabase :HbaseContext): Unit = {

    val htable = recommendDatabase.getExistTable()
    htable.setAutoFlush(false, true)

    val familyName = "video_ids"

    val columnName = "ids"

    iter.foreach{
      case(id,recommendlist) => {
        if(!recommendlist.isEmpty){
          val put = new Put(id.toString)
          put.addColumn(familyName, columnName, recommendlist.map(_._1).mkString(","))
          htable.put(put)
        }
      }
    }

    htable.flushCommits()
    htable.close()

  }


}