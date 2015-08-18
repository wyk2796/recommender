package recommend.serving

import recommend.RecomConfigure
import recommend.batch.BatchLayer

/**
 * Created by wuyukai on 15/7/2.
 */
object Main {


  def main(args:Array[String]): Unit = {

    val conf = if (args.isEmpty) {
      new RecomConfigure()
    } else new RecomConfigure(false, args(0))



    val sl = new ServingLayer(conf)
    sys.addShutdownHook(sl.stop())
    sl.start()
  }
}
