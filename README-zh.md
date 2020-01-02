# Recommendation System

推荐总体架构图

![recommender-desgin](http://7xl71l.com1.z0.glb.clouddn.com/recommend_design_recommend.jpg)

推荐架构分为三个层次：
batch ：主要处理离线模型训练并存入hdfs和数据存储入hdfs, 包括历史模型和历史数据管理， 目前只集成ALS


batch层架构图
![batch-desgin](http://7xl71l.com1.z0.glb.clouddn.com/recommend_design_batch.jpg)

speed ： 加载hdfs中生成的离线模型， 对从kafka 输入的实时数据流中的用户提供推荐 并把推荐结果写入hdfs


speed层架构图
![speed-desgin](http://7xl71l.com1.z0.glb.clouddn.com/recommend_design_speed.jpg)


serving ： 主要提供对外推荐服务和 整套推荐候选集， 候选推荐item 排序， 过滤，融合算法逻辑


serving层架构图
![serving-desgin](http://7xl71l.com1.z0.glb.clouddn.com/recommend_design_serving.jpg)


整套框架并发和线程通信操作akka 多线程通信。web使用了spray 轻量级scala web 服务。数据存储主要是hbase和hdfs
目前代码不太完整， 整体框架完成， 但排序策略目前只是一个简单的重排序 在itemFamily会预留排序策略接口， 方面以后扩展

扩展点： batch， speed层 模型扩展， 后期加入 决策树， 聚类，相似计算等算法，serving 主要在推荐候选集合， 推荐结果排序融合部分。
