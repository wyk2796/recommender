package recommend.util

import java.util.Properties


import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import recommend.RecomConfigure

import scala.reflect.ClassTag
import akka.actor.{ActorLogging, Actor}
import recommend.util.KafkaString._
import recommend.util.BaseString._

/**
 * Created by wuyukai on 15/6/29.
 */
class KafkaProduce[V:ClassTag](conf: RecomConfigure) extends Actor with ActorLogging{

  private val props = new Properties()
  props.put(MetadataBrokerList, conf.get(Prifix + MetadataBrokerList))
  props.put(SerializerClass, "recommend.serializer.MessageEncoder")
  props.put(ProducerType, "async")

  val config = new ProducerConfig(props)
  val producer = new Producer[String,V](config)


  private def send(key:String,msg:V,topic:String): Unit = {
    try{
      producer.send(new KeyedMessage[String,V](topic, key, msg))
    } catch{
      case e:Exception => log.error("this message send failly , the reason is: " + e.getMessage)
    }
  }

  def receive = {
    case (key:String,msg:V,topic:String) => {
      send(key, msg, topic)
    }
    case _ => log.info("Kafka Produce receive Wrong Message!!")
  }
}
