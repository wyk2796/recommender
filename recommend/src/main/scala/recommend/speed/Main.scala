package recommend.speed

import recommend.RecomConfigure

/**
 * Created by wuyukai on 15/6/29.
 */
object Main {

  def main(args:Array[String]): Unit ={
    val conf = if(args.isEmpty){
      new RecomConfigure()
    } else new RecomConfigure(false, args(0))

    val sl = new SpeedLayer(conf)

    sys.addShutdownHook(sl.stop())

    sl.start()
  }

}
