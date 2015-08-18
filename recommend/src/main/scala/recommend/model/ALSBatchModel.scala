package recommend.model

/**
 * Created by wuyukai on 15/6/26.
 */


import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.recommendation.{Rating, MatrixFactorizationModel}
import org.apache.spark.mllib.rdd.MLPairRDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import recommend.RecomConfigure
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import scala.collection.mutable
import recommend.util.ModelString._
import recommend.util.BaseString._

class ALSBatchModel extends RecommendItem with RecommendUser{

  def this(model: MatrixFactorizationModel) = {
    this()
    alsModel = model
  }

  private var alsModel:MatrixFactorizationModel = null

  def rank = alsModel.rank




  /**
   * 模型中是否包含该userId
   * @param userId: 用户ID
   * @return 存在true, 不存在false
   **/
  def lookupUser(userId:Int):Boolean = {
    !alsModel.userFeatures.lookup(userId).isEmpty
  }

  def lookupUserAndGet(userId:Int): Array[Double] ={
    val feature = alsModel.userFeatures.lookup(userId)
    if(!feature.isEmpty) feature.head
    else Array.empty[Double]
  }

  /**
   * 模型是否包含该ItemId
   * @param itemId: 产品ID
   **/
  def lookupItem(itemId:Int):Boolean = {
    !alsModel.productFeatures.lookup(itemId).isEmpty
  }

  def lookupItemAndGet(itemId:Int): Array[Double] ={
    val feature = alsModel.productFeatures.lookup(itemId)
    if(!feature.isEmpty) feature.head
    else Array.empty[Double]
  }

  /**
   * 获取用户数
   * @return 返回用户个数
   * */
  def userNum = alsModel.userFeatures.count()

  /**
   * 获取物品个数
   * @return 返回产品个数
   * */
  def itemNum = alsModel.productFeatures.count()

  /**
   * 获取用户特征列表
   * @return 用户特征列表
   **/
  def getUserFeature:RDD[(Int,Array[Double])] = alsModel.userFeatures.filter(_ != null)

  /**
   * 获取物品特征列表
   * @return 物品特征列表
   **/
  def getItemFeature:RDD[(Int,Array[Double])] = alsModel.productFeatures.filter(_ != null)


  /**
   * 生成模型，模型参数从RecomConfigure中获取
   * @param trainData:训练模型使用的数据
   * @param conf: 从中获取模型参数
   * @return ALSBatchModel 批处理模型
   **/
  def buildModel(trainData:RDD[Rating], conf: RecomConfigure): this.type = {
    val iteration = conf.getInt(Prifix + AlsIteration, 10)
    val rank = conf.getInt(Prifix + AlsRank, 10)
    alsModel = ALS.train(trainData,rank,iteration)
    this
  }

  /**
   * 保存生成的模型
   * @param path: 模型保存路径
   * @param sc: SparkContext
   **/
  def saveModele(sc: SparkContext, path:String): Unit ={
    if(alsModel != null){
      alsModel.save(sc, path)
    }

  }


  /**
   * 通过路径加载已生成的模型
   * @param path: 模型加载路径
   * @param sc: SparkContext
   **/
  def loadModel(sc:SparkContext, path:String): this.type ={
    try{
      alsModel = MatrixFactorizationModel.load(sc,path)
    }catch {
      case e:Exception => throw new Exception (e.getMessage())
    }
    this
  }

  /**获得所有的用户Id*/
  def getAllUserId():RDD[Int] = {
    alsModel.userFeatures.keys.filter(_ != null)
  }

  /**获得所有的产品Id*/
  def getAllItemId():RDD[Int] = {
    alsModel.productFeatures.keys.filter(_ != null)
  }


  /**
   * 获取用户体征值
   **/
  def getUserFeatures(userId:Int): Array[Double] = {
    val features = alsModel.userFeatures.lookup(userId)
    if(!features.isEmpty) features.head
    else Array.empty[Double]
  }

  /**
   * 获取产品特征值
   **/
  def getItemFeatures(itemId:Int): Array[Double] = {
    val features = alsModel.productFeatures.lookup(itemId)
    if(!features.isEmpty) features.head
    else Array.empty[Double]
  }

  /**
   * 预测当用户评分
   **/
  def predict(userId: Int, itemId: Int): Double = {
    alsModel.predict(userId,itemId)
  }

  /**
   * 批量预测用户对产品的评分
   **/
  def predict(usersItems: RDD[(Int,Int)]): RDD[Rating] = {
    alsModel.predict(usersItems).filter(_ != null)
  }
  /**
   * 向用户推荐产品
   **/
  def recommendItemById(userId:Int, itemNum:Int = 1000): Array[(Int,Double)] = {
    val feature = alsModel.userFeatures.lookup(userId)
    if(feature.isEmpty) return Array.empty[(Int,Double)]
    ALSBatchModel.recommend(feature.head, alsModel.productFeatures, itemNum)
  }

  def recommendItemByFeatures(userFeatures: Array[Double], itemNum: Int = 1000): Array[(Int,Double)] = {
    if(userFeatures.isEmpty) return Array.empty[(Int,Double)]
    require(userFeatures.length == alsModel.rank, "用户的特征值数必须一致")
    ALSBatchModel.recommend(userFeatures, alsModel.productFeatures, itemNum)
  }

  /**
   * 向产品推荐用户
   **/
  def recommendUserById(itemId: Int, userNum: Int = 1000): Array[(Int,Double)] = {
    val feature = alsModel.productFeatures.lookup(itemId)
    if(feature.isEmpty) return Array.empty[(Int,Double)]
    ALSBatchModel.recommend(feature.head, alsModel.userFeatures, userNum)
  }

  def recommendUserByFeatures(itemFeatures: Array[Double], userNum: Int = 1000): Array[(Int,Double)] = {
    if(itemFeatures.isEmpty) return Array.empty[(Int,Double)]
    require(itemFeatures.length == alsModel.rank, "产品的特征值数必须一致")
    ALSBatchModel.recommend(itemFeatures, alsModel.productFeatures, userNum)
  }


  /**
   * 向所有的用户推荐产品
   **/
  def recommendItemForUsers(itemNum: Int): RDD[(Int, Array[(Int,Double)])] = {
    ALSBatchModel.recommendAll(alsModel.rank, alsModel.userFeatures, alsModel.productFeatures, itemNum)
  }

  /**
   * 向所有的产品推荐用户
   **/
  def recommendUsersForItems(userNum: Int): RDD[(Int, Array[(Int,Double)])] = {
    ALSBatchModel.recommendAll(alsModel.rank, alsModel.productFeatures, alsModel.userFeatures, userNum)
  }


  /**
   * 给批量用户推荐物品
   * @param features 用户特征向量
   * @param itemNum 推荐物品数量
   * */
  def recommendForAllUser(features: RDD[(Int, Array[Double])], itemNum: Int): RDD[(Int, Array[(Int,Double)])] ={
    ALSBatchModel.recommendAll(alsModel.rank, features, alsModel.productFeatures, itemNum)
  }

  /**只适合数据量较小的模型， 数据量过大慎用*/
  @deprecated
  def toSpeedModel():ALSSpeedModel = {

    val speedModel = new ALSSpeedModel(alsModel.rank)
    var userCount = 0
    var itemCount = 0
    alsModel.userFeatures.collect().foreach { case(userId, feature) =>
      speedModel.setUserFeature(userId,feature)
        userCount += 1
    }
    alsModel.productFeatures.collect().foreach { case(itemId, feature) =>
      speedModel.setItemFeature(itemId, feature)
        itemCount += 1
    }
    println("The number of user is: " + userCount + "\nThe number of item is: " + itemCount)
    speedModel
  }

  /**加载模型到内存*/
  def persist(): Unit ={
    alsModel.userFeatures.cache()
    alsModel.productFeatures.cache()
  }

  /**从内存移除模型*/
  def unpersist(): Unit ={
    alsModel.userFeatures.unpersist()
    alsModel.productFeatures.unpersist()
  }




}


object ALSBatchModel extends Serializable{

 implicit def tripleToRating(tri: RDD[(String, String, String)]): RDD[Rating] = {

   tri.map(lines => new Rating(lines._1.toInt, lines._2.toInt, lines._3.toDouble))
 }


  /**
   * 通过单个用户(产品)特征推荐
   * */
  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      num: Int): Array[(Int, Double)] = {
    val scored = recommendableFeatures.map { case (id, features) =>
      (id, blas.ddot(features.length, recommendToFeatures, 1, features, 1))
    }
    scored.top(num)(Ordering.by(_._2))
  }

  /**
   * 向所有的用户(产品)推荐
   **/
  private def recommendAll(
      rank:Int,
      srcFeatures: RDD[(Int, Array[Double])],
      dstFeatures: RDD[(Int, Array[Double])],
      num: Int): RDD[(Int, Array[(Int,Double)])] = {
    val srcBlocks = blockify(rank, srcFeatures)
    val dstBlocks = blockify(rank, dstFeatures)

    val ratings = srcBlocks.cartesian(dstBlocks).flatMap {
      case ((srcIds, srcFactors),(dstIds,dstFactors)) =>
        val m = srcIds.length
        val n = dstIds.length
        val ratings: DenseMatrix = srcFactors.transpose.multiply(dstFactors)
        val output = new Array[(Int,(Int,Double))](m * n)
        var k = 0
        foreachActive(ratings){ (i, j ,r) =>
          output(k) = (srcIds(i), (dstIds(j), r))
          k += 1
        }
        output.toSeq
    }

    ratings.topByKey(num)(Ordering.by(_._2))
  }

  //test dm的值是否改变
  private def foreachActive(dm: DenseMatrix)(f: (Int, Int, Double) => Unit): Unit = {
    if(!dm.isTransposed) {
      var j = 0
      while(j < dm.numCols){
        var i = 0
        val indStart = j * dm.numRows
        while (i < dm.numRows){
          f(i, j, dm.values(indStart + i))
          i += 1
        }
        j += 1
      }
    } else {
      var i = 0
      while (i < dm.numRows){
        var j = 0
        val indStart = i * dm.numCols
        while(j < dm.numCols) {
          f(i, j, dm.values(indStart + j))
          j += 1
        }
        i += j
      }
    }
  }


  /**
   * Blockifies features to user level-3 BLAS
   **/

  private def blockify(
      rank: Int,
      features: RDD[(Int,Array[Double])]): RDD[(Array[Int], DenseMatrix)] = {
    val blockSize = 4096
    val blockStorage = rank * blockSize
    features.mapPartitions{ iter =>
      iter.grouped(blockSize).map{ grouped =>
        val ids = mutable.ArrayBuilder.make[Int]
        ids.sizeHint(blockSize)
        val factors = mutable.ArrayBuilder.make[Double]
        factors.sizeHint(blockStorage)
        var i = 0
        grouped.foreach{case (id, factor) =>
          ids += id
          factors ++= factor
          i += 1
        }
        (ids.result(), new DenseMatrix(rank, i, factors.result()))
      }

    }
  }



  def trainingModel(trainData:RDD[Rating], conf: RecomConfigure): ALSBatchModel = {
    val iteration = conf.getInt(Prifix + AlsIteration, 10)
    val rank = conf.getInt(Prifix + AlsRank, 10)
    new ALSBatchModel(ALS.train(trainData,rank,iteration))
  }


}