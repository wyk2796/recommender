package recommend.batch

import recommend.RecomConfigure

/**
 * Created by wuyukai on 15/6/29.
 */
object Main {

  def main(args:Array[String]): Unit = {

    val conf = if(args.isEmpty){
      new RecomConfigure()
    } else new RecomConfigure(false, args(0))

    val bl = new BatchLayer(conf)

    sys.addShutdownHook(bl.stop())

    bl.start()

  }

}
