package recommend.batch

import akka.actor.ActorRef
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS,Rating}
import org.apache.spark.rdd.RDD
import recommend.RecomConfigure
import org.apache.hadoop.conf.Configuration
import recommend.dataprocessing.{DataProcessing, SampleDataProcessing}
import recommend.util.{BatchUpdateMessage, KafKaTopics}
import scala.concurrent.Future
import recommend.util.BaseString._
import recommend.util.ModelString._



/**
 * Created by wuyukai on 15/6/29.
 */
class ALSModelActor(sc:SparkContext, recomConf: RecomConfigure , kafkaSender:ActorRef) {

  val saveRootPath = recomConf.get(Prifix + BatchDataPath)
  val modelSavePath = recomConf.get(Prifix + BatchModelPath)
  val limiteDateOfHistoryData = recomConf.getInt(Prifix + BatchHistoryInterval , 10)
  val computerInterval = recomConf.getInt(Prifix + BatchComputerInterval, 300000)
  val dataSplit = recomConf.get(Prifix + DataSplit ,",")

  val rank = recomConf.getInt(Prifix + AlsRank, 5)
  val iterations = recomConf.getInt(Prifix + AlsIteration, 10)

  implicit val global = scala.concurrent.ExecutionContext.global


  var runner = true


  //runner
  def run(): Unit = {

    Future{
      while(runner){
        println("begin trainging")
        val pathSet = ALSModelActor.getDataPath(saveRootPath,limiteDateOfHistoryData)
        println("training data path is following")
        pathSet.foreach(println)
        val rdd = ALSModelActor.getTrainingData(sc,pathSet)
        if(rdd != null && !rdd.isEmpty()){
          try{
            
            val trainingData = ALSModelActor.dataProcessing(rdd,new SampleDataProcessing(recomConf))
            println("the count " + trainingData.count())
            val model = ALS.train(trainingData,rank,iterations)
            val time = recommend.util.Utils.generateDate("YYYYMMdd/HH-mm-ss")
            val modelPath = modelSavePath + "/" + time + "_Model"
            model.save(sc,modelPath)
            val msg = new BatchUpdateMessage(modelPath)
            println("Send the batch model update message to kafka")
            kafkaSender ! ("ModelPath", msg, KafKaTopics.BatchModelUpdate)
          }catch {
            case e:Exception => println("error the reason is: " + e.getMessage)
          }
        } else println("training data is empty , can't compute the model please wait ")
        Thread.sleep(computerInterval)
      }
    }
  }

  def stop(): Unit ={
    runner = false
  }



}

object ALSModelActor{

  /**
   * 获取训练数据
   * @param sc:spark上下文
   * @param pathSet:数据路径集合
   * @return 获取得到的数据
   * */
  private def getTrainingData(sc:SparkContext, pathSet:Array[String]): RDD[String] = {

    if(!pathSet.isEmpty) {
      val data = for(path <- pathSet) yield sc.textFile(path)
      if(data.length > 1) data.reduce(_.union(_))
      data(0)
    } else null
  }


  /**
   * 数据预处理
   * @param data:处理数据
   * @param process:数据预处理类
   * */
  private def dataProcessing(data:RDD[String],process: DataProcessing): RDD[Rating] = {
    process.trainsformation(data)

  }

  private def getDataPath(saveRootPath:String, limitDay:Int):Array[String] = {
    validPath(saveRootPath)
    val hdfsConf = new Configuration()
    val savePath = new Path(saveRootPath)
    try{
      val fs = savePath.getFileSystem(hdfsConf)
      fs.listStatus(savePath)
        .sortWith(_.getModificationTime > _.getModificationTime)
        .take(limitDay).map(_.getPath.toString)
    } catch{
      case e:Exception => println("Get data path create an error")
                          Array.empty[String]
    }
  }

  private def validPath(savePath:String) {
    if(!savePath.startsWith("hdfs://"))
      throw new Exception("data save path" + savePath + "is not valid")
  }


}
