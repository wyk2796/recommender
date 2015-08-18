/**
 * Created by wuyukai on 15/6/25.
 */

package recommend.util

import java.io.{ObjectInputStream,ObjectOutputStream}

import org.apache.hadoop.conf.{Configuration => HadoopConf}

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{SequenceFile,Text}

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

import scala.collection.mutable.ArrayBuffer

object HadoopIO {



  def createHadoopPath(path:String): Unit ={

    try{
      val conf = new HadoopConf()
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(conf)

      if(!fs.exists(hdfsPath)) {
        fs.mkdirs(hdfsPath)
      }
    } catch {
      case e:Exception => throw new Exception(e.getMessage())
    }
  }

  def writeRecom(matrix:MatrixFactorizationModel,path:String) {

    try {

      /* Write MatrixFactorizationModel */
      val conf = new HadoopConf()
      val fs = FileSystem.get(conf)

      val oos = new ObjectOutputStream(fs.create(new Path(path)))
      oos.writeObject(matrix)

      oos.close

    } catch {
      case e:Exception => throw new Exception(e.getMessage())
    }

  }

  def readRecom(path:String):MatrixFactorizationModel = {

    try {

      /* Read Matrix FactorizationModel */
      val conf = new HadoopConf()
      val fs = FileSystem.get(conf)

      val ois = new ObjectInputStream(fs.open(new Path(path)))
      val matrix = ois.readObject().asInstanceOf[MatrixFactorizationModel]

      ois.close()

      matrix

    } catch {
      case e:Exception => throw new Exception(e.getMessage())

    }

  }

  def writeToHadoop(ser:String,file:String) {

    try {

      val conf = new HadoopConf()
      val fs = FileSystem.get(conf)

      val path = new Path(file)
      val writer = new SequenceFile.Writer(fs, conf, path, classOf[Text], classOf[Text])

      val k = new Text()
      val v = new Text(ser)

      writer.append(k,v)
      writer.close()

    } catch {
      case e:Exception => throw new Exception(e.getMessage())

    }

  }


  def writeToHadoop(ser: Iterator[String], file:String) {
    if(ser.isEmpty) return
    try {

      val conf = new HadoopConf()
      val path = new Path(file)
      val fs = path.getFileSystem(conf)

      val output = if(!fs.exists(path)) fs.create(path)
                  else fs.append(path)
      ser.foreach{
        value => {
          val str = value + "\n"
          output.write(str.getBytes("UTF-8"))
          output.flush()
        }
      }

      output.close()

    }catch {
      case e:Exception => throw new Exception(e.getMessage())

    }
  }

  def readFromHadoop(file:String):String = {

    try {

      val conf = new HadoopConf()
      val fs = FileSystem.get(conf)

      val path = new Path(file)

      val reader = new SequenceFile.Reader(fs,path,conf)

      val k = new Text()
      val v = new Text()

      reader.next(k, v)
      reader.close()

      v.toString

    } catch {
      case e:Exception => throw new Exception(e.getMessage())

    }
  }

  def readFromHadoopFile(file:String):Iterator[String] = {

    val data = new ArrayBuffer[String]

    try {

      val conf = new HadoopConf()
      val path = new Path(file)

      val fs = path.getFileSystem(conf)

      val inPutStream: FSDataInputStream = fs.open(path)

      val readLine = inPutStream.readLine


      while(null != readLine) {
        data += readLine
      }

      inPutStream.close()

    }catch {
      case e:Exception => throw new Exception(e.getMessage)
    }
    data.toIterator
  }

  /**
   * 获取最新生成的模型，用于初始化ModelManage
   * @param rootModelpath: 模型存储根目录
   * return 最进一次生成的模型 ， 没有则返回为 null
   * */
  def getLastModelPath(rootModelpath: String): String ={
    val conf = new HadoopConf()
    val rootPath = new Path(rootModelpath)
    val rootFs = rootPath.getFileSystem(conf)
    if(rootFs.exists(rootPath)){
      val rootChildren = rootFs.listStatus(rootPath)
      val pathChild = if(!rootChildren.isEmpty) rootChildren.last.getPath
      else return null
      val childrenFs = pathChild.getFileSystem(conf)
      childrenFs.listStatus(pathChild).last.getPath.toString
    } else null
  }


}