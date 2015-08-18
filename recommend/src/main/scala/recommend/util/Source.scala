package recommend.util

/**
 * Created by wuyukai on 15/7/25.
 */
object BaseString {
  // 命名前缀
  val Prifix = "kaka."

  //批处理数据存放路径 必须是hdfs路径
  val BatchDataPath = "batch.data.path"

  //批处理数据生成模型存放路径
  val BatchModelPath = "batch.model.path"

  //使用历史数据时间窗口, 例如 设值为3 表示使用从当前开始之前3天得历史数据
  val BatchHistoryInterval = "batch.history.interval"

  //批处理数据模型训练间隔时间
  val BatchComputerInterval = "batch.computer.interval"

  //数据分隔符
  val DataSplit = "data.split"

  //spark streaming 接收kafka流的 groupid
  val StreamingGroupId = "streaming.group.id"

  //spark streaming 接收流时间间隔
  val StreamingInterval = "streaming.interval"

  //spark streaming 接收kafka流的topic
  val StreamingTopic = "streaming.topic"

  //spark streaming 接收kafka流的 并发数
  val StreamingPartitionNum = "streaming.partition.num"

  //操作数据特征字段数
  val OptionFeatures = "option.features"

  //serving 的延迟时间
  val ServingTimeout = "serving.timeout"

  //serving 接收请求的并发数
  val ServingThreadNum = "serving.thread.num"

  //serving 绑定ip地址
  val ServingIp = "serving.ip"

  //端口号
  val ServingPort = "serving.port"

  //kafka 的group id
  val GroupId = "group.id"

}

object ModelString{

  //als 分解特征数
  val AlsRank = "model.als.rank"

  //als 训练迭代次数
  val AlsIteration = "model.als.iteration"

}

object KafkaString{

  //zookeeper 连接ip:host
  val ZookeeperConnect = "zookeeper.connect"

  //zookeeper 回话延时
  val ZookeeperSessionTimeoutMs = "zookeeper.session.timeout.ms"

  //zookeeper 同步间隔
  val ZookeeperSyncTimeMs = "zookeeper.sync.time.ms"

  val AutoCommitIntervalMs = "auto.commit.interval.ms"

  //kafka broker ip:host
  val MetadataBrokerList = "metadata.broker.list"

  //生产消息连接方式 sync async
  val ProducerType = "producer.type"

  //消息序列化类
  val SerializerClass = "serializer.class"

}
