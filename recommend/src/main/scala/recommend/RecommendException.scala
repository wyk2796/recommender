package recommend

/**
 * Created by wuyukai on 15/7/24.
 */
class RecommendException(msg:String = "Recommend Error") extends Exception(msg)

class ALSRecommendException(msg:String = "ALSRecommend Error") extends Exception(msg)

class SpeedModelException(msg:String = "Speed Model Error") extends ALSRecommendException(msg)

class BatchModelException(msg:String = "Batch Model Error") extends ALSRecommendException(msg)

class CacheException(msg:String = "Cache Error") extends Exception(msg)

class ScheduleException(msg:String = "Schedule Error") extends Exception(msg)

class TestException(msg:String = "Test Error") extends Exception(msg)


