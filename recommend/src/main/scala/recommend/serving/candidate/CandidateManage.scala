package recommend.serving.candidate

/**
 * Created by wuyukkai on 15/7/31.
 */

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.hadoop.hbase.client._
import recommend.RecomConfigure
import recommend.database.HbaseContext
import recommend.util.BaseString._
import recommend.util.KafkaString._
import recommend.database.HbaseContext._

import scala.collection.mutable.HashMap
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


class CandidateManage(conf:RecomConfigure) extends Actor with ActorLogging{

  private val CandidateLists = HashMap[Int,(ActorRef,String)]()

  private val zkIp = conf.get(Prifix +ZookeeperConnect)

  private val candListInformation = new HbaseContext(zkIp,"CandidateListInformation")

  private val candParamsForUser = new HbaseContext(zkIp,"CandidateParamsForUser")

  implicit private val timeout = Timeout(1 seconds)

  implicit val gc = scala.concurrent.ExecutionContext.global



  override def preStart(): Unit = {
    //read Candidate data from hbase
    //initial candidates

    if(!candListInformation.isExist()) {
      val columns = Array("candidate_information")
      if(candListInformation.createTable(columns)) log.info("CandidateManage: Table CandidateListImformation create successful")
    }

    if(!candParamsForUser.isExist()) {
      val columns = Array("candidate_list_weight", "candidate_list_offset")
      if(candParamsForUser.createTable(columns)) log.info("CandidateManage: Table CandidateParamsForUser create successful")
    }

    val iTable = candListInformation.getExistTable()
    val familyName = "candidate_information"
    val scan = new Scan()
    val resultScan = iTable.getScanner(scan).iterator()

    while(resultScan.hasNext){
      val result = resultScan.next()
      val tableName:String = result.getRow()
      val candidateType:String = result.getValue(familyName,"candidate_list_type")
      val storeType:String = result.getValue(familyName,"store_type")
      val url:String = result.getValue(familyName, "url")
      val algo:String = result.getValue(familyName, "algo")
      log.info(tableName +"|"+ candidateType +"|"+ storeType +"|"+ url +"|"+ algo)
      createCandidateList(candidateType, storeType, url, tableName, algo)
    }

    log.info("create candidate finished, the num is " + CandidateLists.size)

  }

  def createCandidateList(candidateType:String, storeType:String, url:String, tableName:String, algo:String): Unit = {

    candidateType match {
      case "mode" => createModelCandidate(storeType,url,tableName, algo)

      case "list" => createListCandidate(storeType,url,tableName)

      case _ => log.error("candidate_type is wrong, create candidate is fail. ")
    }
  }

  /**
   * 生成模型类别候选集
   * */
  def createModelCandidate(storeType:String, url:String, tableName:String, algo:String): Unit = {
    val id = createNewId
    (storeType, algo) match {
      case ("hbase", "als") => {
        val hc = new HbaseContext(url,tableName)
        if(hc.isExist()){
          //val candidate = new ALSCandidateList(hc.getExistTable(),id)
          val candidate = context.actorOf(Props(new ALSCandidateList(hc.getExistTable(),id)), s"CandidateList_${id}")
          insert(id, candidate, "mode")
        }else{
          log.error(s"create CandidateList ${tableName} was failed, because the Candidate in database is not exist!!")
        }
      }
      case ("hbase", "kmean") => log.error("this function will be come in the future")
      case _ => log.error("this is deveopling, will be used in future.")
    }
  }

  /**
   * 生成列表类型候选集
   **/
  def createListCandidate(storeType:String, url:String, tableName:String): Unit ={
    val id = createNewId
    storeType match {
      case "hbase" => {
        val hc = new HbaseContext(url,tableName)
        if(hc.isExist()){
          val candidate = context.actorOf(Props(new TopCandidateList(hc.getExistTable(),id)), s"CandidateList_${id}")
          insert(id, candidate, "list")
        }
        else log.error(s"create CandidateList ${tableName} was failed, because the Candidate in database is not exist!!")
      }
      case "jdbc" => log.error("this function will be come in the future")
      case "file" => log.error("this function will be come in the future")
      case _ => log.error("this function will be come in the future")
    }
  }


  /**
   * 删除候选集列表
   * @param id: 候选集id
   * */
  def deleteCandidateList(id:Int): Unit ={
    CandidateLists -= id
  }

  /**
   * 获选集插入候选集列表
   * @param candidate: 候选集类
   * */
  def insert(id:Int, candidate: ActorRef, candtype:String): Unit = {
    CandidateLists += ((id, (candidate, candtype)))
  }

  /**
   * 生成新的候选集Id
   * */
  private def createNewId:Int = {
    if(!CandidateLists.isEmpty){
      var flag = true
      var tmp:Int = 0
      while(flag) {
        tmp = (math.random * 100000).toInt
        if(CandidateLists.contains(tmp)) flag = false
      }
      tmp
    } else (math.random * 100000).toInt
  }

  /**
   * 得到候选集参数
   **/
  private def getCandidateParams(userid:Int): HashMap[Int,(Double, String)] ={
    val table = candParamsForUser.getExistTable()
    val get = new Get(userid)
    var params = HashMap[Int, (Double,String)]()
    if(table.exists(get)){
      val candIds = CandidateLists.keys
      get.addFamily("candidate_list_weight")
      get.addFamily("candidate_list_offset")
      val result = table.get(get)

      for(id <- candIds){
        try{
          val weight:Double = result.getValue("candidate_list_weight", id)
          val offset:String = result.getValue("candidate_list_offset", id)
          params += ((id,(weight,offset)))
        }catch{
          case e:Exception => log.warning(s"CandidateManage: the ${id} of Candidate not save in the database!")
        }finally {
          table.close()
        }
      }
    }

    params
  }


  /**
   * 从候选集拉取列表
   * */
  def pullListFormCandidate(userid:Int, num: Int): Array[ItemUnit] = {
    val params = getCandidateParams(userid)
    val results = for(candidate <- CandidateLists) yield{
      val (itemNum,offset) = if(!params.isEmpty){
        if(params.contains(candidate._1)) {
          val (tmpW, tmpO) = params(candidate._1)
          ((num * tmpW).toInt,tmpO)
        }else (((1.0000 / CandidateLists.size) * num).toInt ,"0")
      }else{
        (((1.0000 / CandidateLists.size) * num).toInt ,"0")
      }

      candidate._2._2 match {
        case "list" => ask(candidate._2._1, new CandidateListMessage(itemNum, offset.toString))
        case "mode" => ask(candidate._2._1, new CandidateModelMessage(userid,itemNum, offset.toString))
        case _ => throw new Exception("CandidateManage: the CandidateList Type is error")
      }
    }

    val rmp = results.map(r => {
      val crm = r.mapTo[CandidateReturnMessage]
      Await.result(crm,timeout.duration)
    })

    Future{
      rmp.foreach{
        msg => {
          val weight = params(msg.Cid)._1
          params(msg.Cid) = (weight,msg.offset)
        }
      }
    }
    saveParamsToHbase(userid, params)

    rmp.map(_.list).reduce(_ ++ _)
  }

  def saveParamsToHbase(userid:Int, params:HashMap[Int,(Double, String)]): Unit ={

    if(!params.isEmpty){
      val put = new Put(userid)
      params.foreach{
        case(id,(weight,offset)) =>
          put.addColumn("candidate_list_weight", id, weight)
          put.addColumn("candidate_list_offset", id, offset)
      }
      val table = candParamsForUser.getExistTable()
      try{
        table.put(put)
      }catch {
        case e:Exception => log.error("CandidateManage: Save user params to hbase create an error" + e.getStackTraceString)
      }finally {
        table.close()
      }
    }else log.info(s"$userid params is empty")
  }

  def receive = {
    case CreateCandidateMessage(candidateType,storeType,url,tableName,algo) => createCandidateList(candidateType,storeType,url,tableName,algo)
    case DeleteCandidateMessage(id) => deleteCandidateList(id)
    case PullListMessage(id, num, pType) => sender ! pullListFormCandidate(id, num)
    case _ => log.error("CandidateManage: receive wrong message!!")
  }





}
