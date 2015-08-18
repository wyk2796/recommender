package recommend.batch

import akka.actor.{Props, ActorSystem}
import org.apache.spark.{SparkContext, SparkConf}
import recommend.RecomConfigure
import recommend.util.KafkaProduce


/**
 * Created by wyk on 15/6/25.
 */
class BatchLayer (conf: RecomConfigure){


  val sconf = new SparkConf()
  sconf.setAll(conf.getVariableWithKeyStart("kaka.spark").map{case(a,b) => (a.substring(a.indexOf(".") + 1),b)})
  val sc = new SparkContext(sconf)


  val batchLayerSys = ActorSystem("BatchLayer")
  val kafkaSender = batchLayerSys.actorOf(Props(new KafkaProduce[AnyRef](conf)))
  val streamActor = new SaveDataActor(sc, conf)
  val alsModelActor = new ALSModelActor(sc, conf, kafkaSender)

  /**
   * 开始运行
   **/
  def start(): Unit = {

    try{
      streamActor.run()
      alsModelActor.run()
    }catch{
      case e:Exception => println("the program create an error and will be stop ")
        stop()
        sys.exit()
    }


  }

  /**
   * 停止运行
   **/
  def stop(): Unit ={

    streamActor.stop()
    alsModelActor.stop()


  }





}
