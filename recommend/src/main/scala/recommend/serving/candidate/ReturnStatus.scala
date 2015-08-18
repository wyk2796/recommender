package recommend.serving.candidate

/**
 * Created by apple on 15/7/31.
 */
object ReturnStatus extends Enumeration{

  type ReturnStatus = Value

  val Successful, Fail, Error = Value

}

object PullType extends Enumeration{

  type PullType = Value

  val ReturnDirect, ReturnToDataBase = Value
}
