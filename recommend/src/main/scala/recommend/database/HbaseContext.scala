package recommend.database

import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable}
import org.apache.hadoop.hbase.util.Bytes
import recommend.util.KafkaString._


/**
 * Created by apple on 15/7/30.
 */
class HbaseContext(zkConnect: String, tableName: String) extends Serializable{

  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set(ZookeeperConnect,zkConnect)

  val tn = TableName.valueOf(tableName)


//  val table = new HTable(hbaseConf,tableName)


  def createTable(familyName:Array[String]):Boolean = {
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin


    val tdesc = new HTableDescriptor(tn)

    for(fName <- familyName) {
      tdesc.addFamily(new HColumnDescriptor(fName))
    }
    admin.createTable(tdesc)
    val avail = admin.isTableAvailable(tn)
    admin.close()
    conn.close()
    avail
  }

  def deleteTable(): Unit = {

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    admin.disableTable(tn)
    admin.deleteTable(tn)
    admin.close()
    conn.close()
  }

  def isExist():Boolean = {

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val hasExist = admin.tableExists(tn)
    admin.close()
    conn.close()
    hasExist
  }

  def getExistTable(): HTable = {
    new HTable(hbaseConf, tn)
  }



}


object HbaseContext {

  implicit def double2bytes(d:Double):Array[Byte] = Bytes.toBytes(d)

  implicit def string2bytes(s:String):Array[Byte] = Bytes.toBytes(s)

  implicit def int2bytes(i:Int):Array[Byte] = Bytes.toBytes(i)

  implicit def long2bytes(l:Long):Array[Byte] = Bytes.toBytes(l)

  implicit def any2bytes(a:Any):Array[Byte] = a match{
    case x:Int => int2bytes(x)
    case x:String => string2bytes(x)
    case x:Double => double2bytes(x)
    case x:Long => long2bytes(x)
    case _ => Array.empty[Byte]
  }

  implicit def bytes2string(bytes: Array[Byte]):String = Bytes.toString(bytes)

  implicit def bytes2int(bytes: Array[Byte]):Int = Bytes.toInt(bytes)

  implicit def bytes2double(bytes: Array[Byte]):Double = Bytes.toDouble(bytes)

  implicit def bytes2long(bytes: Array[Byte]):Long = Bytes.toLong(bytes)

}