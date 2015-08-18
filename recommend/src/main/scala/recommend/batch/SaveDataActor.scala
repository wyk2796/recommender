package recommend.batch

import org.apache.spark.SparkContext
import recommend.RecomConfigure
import recommend.util.BaseString._

/**
 * Created by wuyukai on 15/6/29.
 */
class SaveDataActor(sc: SparkContext,recomConf: RecomConfigure){

  val saveRootPath = recomConf.get(Prifix + BatchDataPath)

  val stream = new BatchStreaming(sc,recomConf)


  /**
   * 开始运行Spark
   * */
  def run(): Unit = {

    try{
      stream.saveStreamToHdfs(saveRootPath)
      stream.start()
    }catch{
      case e:Exception => stream.stop()
        throw new Exception(e.getMessage)
    }
  }



  def createPath(path:String): Unit = {
    recommend.util.HadoopIO.createHadoopPath(path)
  }

  def stop(): Unit ={

    stream.stop()

  }



}
