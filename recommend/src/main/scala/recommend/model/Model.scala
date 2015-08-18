package recommend.model

/**
 * Created by wuyukai on 15/7/25.
 */

trait Model {



}


trait RecommendItem {

  def recommendItemById(userId:Int, itemNum:Int = 1000): Array[(Int,Double)]

  def recommendItemByFeatures(userFeatures: Array[Double], itemNum: Int = 1000): Array[(Int,Double)]



}

trait RecommendUser {

  def recommendUserById(itemId: Int, userNum: Int = 1000): Array[(Int,Double)]

  def recommendUserByFeatures(itemFeatures: Array[Double], userNum: Int = 1000): Array[(Int,Double)]

}

trait StatisticModel {

}


