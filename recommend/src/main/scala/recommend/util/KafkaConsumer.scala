package recommend.util

import java.util.Properties

import kafka.consumer.{Consumer, ConsumerConfig}
import recommend.RecomConfigure
import recommend.serializer.MessageDecoder

import akka.actor.ActorRef
import scala.concurrent.Future
import recommend.util.BaseString._
import recommend.util.KafkaString._

/**
 * Created by wuyukai on 15/6/30.
 */
class KafkaConsumer (conf: RecomConfigure){

  private val props = new Properties()
  props.put(ZookeeperConnect, conf.get(Prifix + ZookeeperConnect))
  props.put(GroupId, conf.get(Prifix + GroupId))
  props.put(ZookeeperSessionTimeoutMs , conf.get(Prifix + ZookeeperSessionTimeoutMs ,"400"))
  props.put(ZookeeperSyncTimeMs, conf.get(Prifix + ZookeeperSyncTimeMs,"200"))
  props.put(AutoCommitIntervalMs,conf.get(Prifix + AutoCommitIntervalMs, "1000"))

  private val config = new ConsumerConfig(props)

  private val consumer = Consumer.create(config)

  implicit val geoble = scala.concurrent.ExecutionContext.global

  def run(topicMap: Map[String,Int], consumerActor:ActorRef): Unit = {
    val consumerMap = consumer.createMessageStreams[AnyRef,AnyRef](topicMap ,new MessageDecoder,new MessageDecoder)


    for(topic <- topicMap){
      val streams = consumerMap.get(topic._1).get
      for(stream <- streams) {

        Future{
          val iter = stream.iterator()
          while(iter.hasNext()){
            val msg = iter.next().message()
            consumerActor ! msg
          }

        }
      }
    }

  }

//  def stop(): Unit ={
//    consumerSys.shutdown()
//  }

}

//private class receiveActor(val stream:KafkaStream[AnyRef, AnyRef],consumerActor:ActorRef) extends Actor {
//  def act(): Unit = {
//    val iter = stream.iterator()
//
//    while(iter.hasNext()){
//      val msg = iter.next().message()
//      consumerActor ! msg
//    }
//  }
//}