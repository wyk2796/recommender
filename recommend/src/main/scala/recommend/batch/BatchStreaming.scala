package recommend.batch

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import recommend.RecomConfigure
import recommend.util.BaseString._
import recommend.util.KafkaString._

/**
 * Created by wuyukai on 15/6/29.
 */
class BatchStreaming(sc: SparkContext,
                     reconf: RecomConfigure) {

  /**获取zookeeper连接参数 example host1:psot1,host2:post2*/
  val zkQuorum = reconf.get(Prifix + ZookeeperConnect)

  /**获取kafka的group id*/
  val groupId = reconf.get(Prifix + StreamingGroupId)

  val interval = reconf.getInt(Prifix + StreamingInterval,500)

  /**获取kafka topic id*/
  val topic = reconf.get(Prifix + StreamingTopic)

  /**获取接受到数据的分块个数*/
  val numPartition = reconf.getInt(Prifix + StreamingPartitionNum, 5)

  val ssc = new StreamingContext(sc, Seconds(interval))

  val stream = KafkaUtils.createStream(ssc,zkQuorum,groupId, Map(topic -> numPartition))

  println("kaka.zookeeper.connect:" + zkQuorum + ":")
  println("kaka.streaming.group.id:" + groupId + ":")
  println("kaka.streaming.interval:" + interval + ":")
  println("kaka.streaming.topic:" + topic + ":")
  println("kaka.streaming.partition.num" + numPartition + ":")

  /**对流数据操作*/
   def rddOption(time: Long,f:RDD[String] => Unit): Unit ={
    val rddData = stream.compute(Time(time))
    if(rddData != None) f(rddData.get.map(_._2))

  }

  def getRDD(times:Long):RDD[String] = {
    val rddData = stream.compute(new Time(times))
    if(rddData != None) rddData.get.map(_._2)
    else null
  }



  def saveStreamToHdfs(path:String): Unit = {
    stream.foreachRDD(BatchStreaming.rddToHdfs(_,path,sc))
  }




  def start(): Unit ={
    ssc.start()
    ssc.awaitTermination()
    println("stream is start")
  }


  def stop(): Unit ={
    ssc.stop()
  }

}

object BatchStreaming{

  /**
   * 保存数据到hdfs
   * @param rdd:从kafka读到得数据流
   * @param path:模型保存路径
   * @param sc:spark上下文
   * */
  def rddToHdfs(rdd:RDD[(String,String)],path :String, sc:SparkContext): Unit ={
    val data = rdd.map(_._2).coalesce(10, false)
    val savePath = path + "/" +recommend.util.Utils.generateDate("YYYYMMdd/HH-")
    val tmp = data.mapPartitionsWithIndex{
      case(index, iter) =>{
        recommend.util.HadoopIO.writeToHadoop(iter, savePath + index)
        iter
      }
    }
    val emptyFun = (iter:Iterator[String]) => 1
    sc.runJob(tmp, emptyFun)
  }

}
