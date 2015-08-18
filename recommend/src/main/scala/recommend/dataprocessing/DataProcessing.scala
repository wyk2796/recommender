package recommend.dataprocessing

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import recommend.RecomConfigure
import recommend.util.BaseString._


/**
 * Created by Yukai on 2015/7/9.
 */
trait DataProcessing extends Serializable{

  def trainsformation(data:RDD[String]):RDD[Rating] = {
    data.map(trainsformation)
  }

  def trainsformation(data:String):Rating


}


class SampleDataProcessing(conf: RecomConfigure) extends DataProcessing{

  val dataSpilt = conf.get(Prifix + DataSplit)
  val optionFeatures = conf.getInt(Prifix + OptionFeatures ,3)
//  val optionWeights = conf.get("kaka.option.features.weights")

  def trainsformation(data:String):Rating = {
    var rat = new Rating(0,0,0.0)
    try{
      val features = data.split(dataSpilt).map(_.toDouble)
      val rating = features.drop(2).reduce(_ + _ * 2)
      rat = new Rating(features(0).toInt, features(1).toInt, rating)
    }catch{
      case e:Exception => println(e.getMessage)
    }
    rat
  }
}
