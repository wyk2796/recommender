package recommend.serializer

/**
 * Created by wuyukai on 15/7/1.
 */


import kafka.serializer.{Encoder,Decoder}
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import kafka.utils.VerifiableProperties

/**
 * 序列化类消息类,把Message Case Class序列化为Byte
 * */
class MessageEncoder(props: VerifiableProperties = null) extends Encoder[AnyRef] {

  def toBytes(t: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    try{
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(t)
      oos.flush()
      val bytes = bos.toByteArray
      oos.close()
      bos.close()
      bytes
    }catch{
      case e:Exception => throw new Exception(e.getMessage)
    }
  }
}

/**
 * 反序列化消息类,把Array[Byte]反序列化为Message Case Class
 * */
class MessageDecoder(props: VerifiableProperties = null) extends Decoder[AnyRef]{

  def fromBytes(bytes: Array[Byte]):AnyRef = {
    try{
      val bis = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bis)
      val obj = ois.readObject()
      ois.close()
      bis.close()
      obj.asInstanceOf[AnyRef]
    } catch {
      case e: Exception => throw new Exception(e.getMessage)
    }
  }
}