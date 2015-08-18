package recommend.serving.schedule

/**
 * Created by Yukai on 2015/7/16.
 */


object ReturnOption extends Enumeration{
  // 推荐结果返回状态, ReturnList: 直接返回结果, UpdateCache:需求更新缓存数据 ,
  // FindFail:缓存数据抓取失败, WriterToCache:写入缓存 , Error:产生错误

  type ReturnStatus = Value

  val ReturnList, UpdateCache, UpdateDatabase,CacheFindFail, DatabaseFindFail, WriterToCache , Error, MessageError = Value

}


object ActorStatus extends Enumeration{
  // 推荐线程状态, Busying:在忙 ,Available:可用

  type ActorStatus = Value

  val Busying , Available = Value

}

