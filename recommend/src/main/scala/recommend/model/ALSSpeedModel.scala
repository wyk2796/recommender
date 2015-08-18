package recommend.model


import recommend.SpeedModelException

import scala.collection.mutable.{ArrayBuffer, HashMap}

import java.util.concurrent.locks.{ReadWriteLock, ReentrantReadWriteLock}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

/**
 * Created by wuyukai on 15/6/26.
 */
class ALSSpeedModel(private val features:Int) extends RecommendItem with RecommendUser {


  /** 用户特征矩阵*/
  private val userFeature = new HashMap[Int, Array[Double]]()

  /** 产品特征矩阵**/
  private val itemFeature = new HashMap[Int, Array[Double]]()

  /** 新加入的用户集合 */
  private val recentNewUser = ArrayBuffer[Int]()

  /** 新加入的产品集合*/
  private val recentNewItem = ArrayBuffer[Int]()

  /**控制访问userFeature*/
  private val userLock: ReadWriteLock = new ReentrantReadWriteLock()

  /**控制访问itemFeature*/
  private val itemLock: ReadWriteLock = new ReentrantReadWriteLock()

  def getFeatures = features


  def lookupUser(userId:Int): Boolean = {
    userLock.readLock().lock()
    val isContains = userFeature.contains(userId)
    userLock.readLock().unlock()
    isContains
  }


  def lookupItem(itemId:Int):Boolean = {
    itemLock.readLock().lock()
    val isContains = itemFeature.contains(itemId)
    itemLock.readLock().unlock()
    isContains

  }


  def getAllUserId():Iterator[Int] = {
    userLock.readLock().lock()
    val userIds = userFeature.keys
    userLock.readLock().unlock()
    userIds.toIterator
  }

  def getAllItemId():Iterator[Int] = {
    itemLock.readLock().lock()
    val itemIds = itemFeature.keys
    itemLock.readLock().unlock()
    itemIds.toIterator
  }

  def getRecentNewUser(): Array[Int] = {
    recentNewUser.toArray
  }

  def getRecentNewItem(): Array[Int] = {
    recentNewUser.toArray
  }


  /**
   * 获取User特征值
   * @param userId:用户Id
   * @return  用户feature
   **/
  def getUserFeatures(userId: Int): Array[Double] = {
    userLock.readLock().lock()
    try
      userFeature(userId)
    catch {
      case e: NoSuchElementException => {
        return Array.empty[Double]
      }
      case e:Exception => throw new SpeedModelException(e.getMessage)
    } finally {
      userLock.readLock().unlock()
    }
  }

  /**
   * 获取Item特征值
   * @param itemId:产品ID
   * @return  产品feature
   **/
  def getItemFeatures(itemId: Int ): Array[Double] = {
    itemLock.readLock().lock()
    try {
      itemFeature(itemId)
    }catch {
      case e: NoSuchElementException => {
        return Array.empty[Double]
      }
      case e: Exception => throw new SpeedModelException("the error  " + e.getMessage)
    }finally {
      itemLock.readLock().unlock()
    }
  }

  /**
   * 设置用户特征
   * @param userId: 用户ID
   * @param feature: 用户特征
   **/
  def setUserFeature(userId: Int, feature: Array[Double]): Unit ={
    require(feature.length == features,s"feature is not fit, require ${features}, but find ${feature.length}")
    userLock.writeLock().lock()
    userFeature(userId) = feature
    userLock.writeLock().unlock()
  }


  /**
   * 设置产品特征
   * @param userId: 产品ID
   * @param feature： 产品特征
   * */
  def setItemFeature(userId: Int, feature: Array[Double]): Unit = {
    require(feature.length == features,s"feature is not fit, require ${features}, but find ${feature.length}")
    itemLock.writeLock().lock()
    itemFeature(userId) = feature
    itemLock.writeLock().unlock()
  }

  /**
   * 移除包含在列表中得userId
   * @param userIds:用户Id列表
   **/
  def removeUserById(userIds: Iterator[Int]): Unit = {
    userLock.writeLock().lock()
    while(userIds.hasNext){
      userFeature.remove(userIds.next())
    }

    userLock.writeLock().unlock()
  }

  def removeUserById(userId: Int): Unit = {
    userLock.writeLock().lock()
    userFeature.remove(userId)
    userLock.writeLock().unlock()
  }

  /**
   * 移除包含在列表中得itemId
   * @param itemIds: 商品Id列表
   **/
  def removeItemById(itemIds: Iterator[Int]): Unit = {
    itemLock.writeLock().lock()
    while(itemIds.hasNext) {
      itemFeature.remove(itemIds.next())
    }
//    recentNewItem.clear()
    itemLock.writeLock().unlock()
  }

  def removeItemById(itemId: Int): Unit = {
    itemLock.writeLock().lock()
    itemFeature.remove(itemId)
    itemLock.writeLock().unlock()
  }

  /**
   * ALSSpeedModel合并
   * @param other: 被合并的ALSSpeedModel
   * @param weight: 新模型合并权重，旧模型为 1 - weight  范围(0 , 1)
   **/
  def mergeOtherModel(other: ALSSpeedModel, weight: Double = 0.5):this.type = {
    require(this.features == other.features, s"new feature: ${other.features} not equal old feature ${other.features}")
    recentNewItem.clear()
    recentNewUser.clear()
    other.userFeature.foreach{
      case(userId, features) => {
        if(lookupUser(userId)) {
          val oldFeature = getUserFeatures(userId)
          val newFeature = for(i <- 0 until features.length) yield features(i) * weight + oldFeature(i) * (1 - weight)
          setUserFeature(userId,newFeature.toArray)
        } else {
          setUserFeature(userId, features)
          recentNewUser += userId
        }
      }
    }
    other.itemFeature.foreach{
      case(itemId, features) => {
        if(lookupItem(itemId)) {
          val oldFeature = getItemFeatures(itemId)
          val newFeature = for(i <- 0 until features.length) yield features(i) * weight + oldFeature(i) * (1 - weight)
          setItemFeature(itemId,newFeature.toArray)
        } else {
          setItemFeature(itemId, features)
          recentNewItem += itemId
        }
      }

    }
    this
  }

  /**
   * 向用户推荐产品
   **/
  def recommendItemById(userId:Int, itemNum:Int = 1000): Array[(Int, Double)] = {
    userLock.readLock().lock()
    val feature = userFeature.get(userId)
    userLock.readLock().unlock()
    if(feature != None) {
      itemLock.readLock().lock()
      val itemlist = ALSSpeedModel.recommend(feature.get, itemFeature, itemNum)
      itemLock.readLock().unlock()
      itemlist
    } else Array.empty[(Int, Double)]
  }

  def recommendItemByFeatures(userFeature:Array[Double], itemNum: Int = 1000): Array[(Int, Double)] = {
    itemLock.readLock().lock()
    val itemlist = ALSSpeedModel.recommend(userFeature, itemFeature, itemNum)
    itemLock.readLock().unlock()
    itemlist
  }

  /**
   * 向产品推荐用户
   **/
  def recommendUserById(itemId:Int, userNum: Int = 1000): Array[(Int, Double)] = {
    itemLock.readLock().lock()
    val feature = itemFeature.get(itemId)
    itemLock.readLock().unlock()
    if(feature != None) {
      userLock.readLock().lock()
      val userlist = ALSSpeedModel.recommend(feature.get, itemFeature, userNum)
      userLock.readLock().unlock()
      userlist
    } else Array.empty[(Int, Double)]
  }


  def recommendUserByFeatures(itemFeature:Array[Double], userNum: Int = 1000): Array[(Int, Double)] = {
    userLock.readLock().lock()
    val userlist = ALSSpeedModel.recommend(itemFeature, userFeature, userNum)
    userLock.readLock().unlock()
    userlist
  }


}


object ALSSpeedModel {

  def recommend(recommendFeature:Array[Double], recommendableFeature:HashMap[Int,Array[Double]], num:Int): Array[(Int,Double)] = {
    if(recommendableFeature.isEmpty || recommendFeature.isEmpty) return Array.empty[(Int,Double)]
    val scored = recommendableFeature.map{
      case(id, feature) => {
        (id, blas.ddot(feature.length, recommendFeature, 1, feature, 1))
      }
    }.take(num).toArray
    if(!scored.isEmpty) scored.take(num).sortWith{case(a,b) => a._2 > b._2}
    else Array.empty[(Int,Double)]
  }


}