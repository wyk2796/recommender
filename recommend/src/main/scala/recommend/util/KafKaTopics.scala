package recommend.util

/**
 * Created by wuyukai on 15/6/29.
 */

case class BatchUpdateMessage (path:String){
  override def toString(): String = {
    "BatchUpdataMessage: \nPath:" + path
  }
}
case class SpeedUpdateMessage(table:String, option:String, id:Int, feature:Array[Double]){
  override def toString(): String = {
    "SpeedUpdataMessage: \nTable:" + table + "\nOption: " + option + "\nid: " + id + "\nFeature: " +
    feature.mkString("|")
  }
}

object KafKaTopics {

  val BatchModelUpdate = "batch_model_update"
  val SpeedModelUpdate = "speed_model_update"

}
