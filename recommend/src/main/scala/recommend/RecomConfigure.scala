package recommend

/**
 * Created by wuyukai on 15/6/25.
 */

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.io.Source._
import scala.collection.JavaConversions._


class RecomConfigure (defaults:Boolean,
                     var path:String = ""){

  /**
   *  创建配置类，从提供的目录读取启动服务所需要的配置参数
   *  default path :$RECOMMEND_HOME/conf/recommend.conf
   **/
  def this() = this(true)

  val settings = new HashMap[String,String]()

  if(defaults) {

    for((key,value) <- mapAsScalaMap(System.getenv()) if key.startsWith("kaka")){
      settings(key.replace("_",".")) = value
    }


  } else {
    createSetting(path)
  }

  private def createSetting(path:String){
    try {
      val propertyFile = fromFile(path,"UTF-8")
      for(line<- propertyFile.getLines) {
        if(!line.startsWith("#") && !line.equals("") && line.contains('=')){
          val kv = line.split("=")
          if(kv.length>2) throw new Exception("ConfigureFile has the wrong input:"+line)
          settings(kv(0).trim)=kv(1).trim
        }
      }

    }catch {
      case e:Exception => throw new Exception(path + " is not exit")
    }
  }



  /**
   * 设置配置变量
   **/
  def set(key: String, value: String): this.type = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value")
    }
    settings(key) = value
    this
  }

  /**
   * 批量设置配置变量
   **/
  def setAll(settings: Traversable[(String, String)]):this.type = {
    this.settings ++= settings
    this
  }


  /**
   * 获取一个变量;
   * 若该变量没有设置，则抛出一个异常
   **/
  def get(key: String): String = {
    settings.getOrElse(key, throw new NoSuchElementException(key))
  }

  /**
   * 获取一个变量, 若没有则使用默认值
   **/
  def get(key: String, defaultValue: String): String = {
    settings.getOrElse(key, defaultValue)
  }


   /**
    * 获取一个Option类型参数
    **/
  def getOption(key:String): Option[String] = {
    settings.get(key)
  }



  /**
   * 获取一个Int 类型的参数， 若没设则获取默认参数
   **/
  def getInt(key: String, defaultValue: Int):Int = {
    settings.get(key).map(_.toInt).orElse(Option(defaultValue)).get
  }

  /**
   * 获取一个Double 类型的参数， 若没设则获取默认参数
   **/
  def getDouble(key: String, defaultValue: Double):Double = {
    settings.get(key).map(_.toDouble).orElse(Option(defaultValue)).get
  }

  /**
   * 获取一个Long 类型的参数， 若没设则获取默认参数
   **/
  def getLong(key: String, defaultValue: Long):Long = {
    settings.get(key).map(_.toLong).orElse(Option(defaultValue)).get
  }

  /**
   * 获取一个Boolean 类型的参数， 若没设则获取默认参数
   **/
  def getBoolean(key: String, defaultValue: Boolean):Boolean = {
    settings.get(key).map(_.toBoolean).orElse(Option(defaultValue)).get
  }


  /**
   * 获取所有的变量
   **/
  def getAll = settings.clone


  /**
   * 判断是否包含该键值的变量
   **/
  def contains(key: String): Boolean = settings.contains(key)


  /**
   * 删除输入键值的变量
   **/
  def remove(key: String): this.type = {
    settings.remove(key)
    this
  }


  /**
   * 通过Key起始字符串过滤变量
   * example: spark.master, spark.driver.memory, spark.cores.max 可以设置
   * keyStart=spark 来过滤出所有spark配置参数
   **/
  def getVariableWithKeyStart(keyStart: String):Map[String, String] = {
    settings.filterKeys(_.startsWith(keyStart))
  }


  /**
   * 打印出所有的配置变量
   **/
  def printAllSetting() {
    println("输入的变量:   ")
    settings.foreach(config=>println("key: "+config._1+"  value: "+config._2))
  }


  /**
   * 复制此类
   **/
  override def clone: RecomConfigure = {
    new RecomConfigure(defaults,path).setAll(settings)
  }

}