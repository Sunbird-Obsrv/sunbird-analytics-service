package org.ekstep.analytics.api.service.experiment

import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import akka.util.Timeout
import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.api.service.experiment.Resolver.ModulusResolver
import org.ekstep.analytics.api.util.{ElasticsearchService, JSONUtils, RedisUtil}
import org.mockito.Mockito.{timeout, _}
// import org.scalatest.Ignore
import redis.clients.jedis.Jedis

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import redis.embedded.RedisServer
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.util.AppConfig

class TestExperimentService extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {
  
  implicit val config = ConfigFactory.load()
  private var redisServer:RedisServer = _;
  private val redisUtil = new RedisUtil()
  private val elasticsearchServiceMock = mock[ElasticsearchService]
  implicit val actorSystem: ActorSystem = ActorSystem("testActorSystem", config)

  private val experimentService = TestActorRef(new ExperimentService(redisUtil, elasticsearchServiceMock)).underlyingActor
  val experimentServiceActorRef = TestActorRef(new ExperimentService(redisUtil, elasticsearchServiceMock))
  val redisIndex: Int = AppConfig.getInt("redis.experimentIndex")
  private val emptyValueExpirySeconds = AppConfig.getInt("experimentService.redisEmptyValueExpirySeconds")
  implicit val executionContext: ExecutionContextExecutor =  scala.concurrent.ExecutionContext.global

  override def beforeAll() {
    super.beforeAll()
    redisServer = new RedisServer(6379);
    redisServer.start();
    ExperimentResolver.register(new ModulusResolver())
  }
  
  override def afterAll() {
    super.afterAll()
    redisServer.stop();
  }

  "Experiment Service" should "return experiment if it is defined for UserId/DeviceId" in {
    reset(elasticsearchServiceMock)

    val userId = "user1"
    val deviceId = "device1"
    val url = "http://sunbird.org/home"
    val experimentData: ExperimentData = JSONUtils.deserialize[ExperimentData](Constants.EXPERIMENT_DATA)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), Some(url), None)
    val key = experimentService.keyGen(Some(deviceId), Some(userId), Some(url), None)

    when(elasticsearchServiceMock.searchExperiment(fields)).thenReturn(Future(Some(experimentData)))

    val result = Await.result(experimentService.getExperiment(Some(deviceId), Some(userId), Some(url), None), 20.seconds)

    result.get.userId should be("user1")
    result.get.key should be("325324123413")
    result.get.id should be("exp1")
    result.get.name should be("first-exp")
    
    val jedis = redisUtil.getConnection(redisIndex);
    jedis.set(key, experimentService.NoExperimentAssigned)
    
    val result2 = Await.result(experimentService.getExperiment(Some(deviceId), Some(userId), Some(url), None), 20.seconds)
    result2 should be (None)
    
    new ModulusResolver().resolve(ExperimentData("", "", "", "", "", "", "", "", 0, 0)) should be (false);
    experimentService.getFieldsMap(None, None, None, None).size should be (0)
    
  }


  it should "return None if no experiment is defined" in {
    reset(elasticsearchServiceMock)
    // no experiment defined for this input
    val userId = "user45"
    val deviceId = "device45"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)

    when(elasticsearchServiceMock.searchExperiment(fields))
      .thenReturn(Future(None))

    val result = experimentService.getExperiment(Some(deviceId), Some(userId), None, None)

    result onComplete {
      case Success(data) =>
        data should be(None)
      case Failure(exception) => exception.printStackTrace()
    }

  }

  it should "should evaluate 'modulus' type experiment and return response" in {
    
    import akka.pattern.ask
    reset(elasticsearchServiceMock)

    implicit val timeout: Timeout = 20.seconds
    // no experiment defined for this input
    val userId = "user3"
    val deviceId = "device3"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)
    val experimentData = JSONUtils.deserialize[ExperimentData](Constants.MODULUS_EXPERIMENT_DATA)

    when(elasticsearchServiceMock.searchExperiment(fields)).thenReturn(Future(Some(experimentData)))

    val result = Await.result((experimentServiceActorRef ? ExperimentRequest(Some(deviceId), Some(userId), None, None))
      .mapTo[Option[ExperimentData]], 20.seconds)

    result.get.userId should be("user3")
    result.get.key should be("modulus-exp-key-2")
    result.get.expType should be("modulus")
    result.get.id should be("modulus-exp-2")
    result.get.name should be("modulus-exp-2")

  }

  it should "evaluate 'modulus' type experiment and return response" in {
    reset(elasticsearchServiceMock)
    // no experiment defined for this input
    val deviceId = "device3"
    val key = experimentService.keyGen(Some(deviceId), None, None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), None, None, None)
    val experimentData = JSONUtils.deserialize[ExperimentData](Constants.MODULUS_EXPERIMENT_WITHOUT_USER_DATA)

    when(elasticsearchServiceMock.searchExperiment(fields)).thenReturn(Future.successful(Some(experimentData)))

    val result = Await.result(experimentService.getExperiment(Some(deviceId), None, None, None), 20.seconds)

    result.get.userId should be(null)
    result.get.key should be("modulus-exp-key-2")
    result.get.expType should be("modulus")
    result.get.id should be("modulus-exp-2")
    result.get.name should be("modulus-exp-2")

  }


  it should "should evaluate 'modulus' type experiment and return none if modulus is false" in {
    reset(elasticsearchServiceMock)
    // no experiment defined for this input
    val userId = "user4"
    val deviceId = "device4"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)
    val experimentData = JSONUtils.deserialize[ExperimentData](Constants.MODULUS_EXPERIMENT_DATA_NON_ZERO)

    when(elasticsearchServiceMock.searchExperiment(fields)).thenReturn(Future(Some(experimentData)))

    Await.result(experimentService.getExperiment(Some(deviceId), Some(userId), None, None), 20.seconds)

  }

  it should "return data from cache if the experiment result is cached" in {
    reset(elasticsearchServiceMock)
    // no experiment defined for this input
    val userId = "user1"
    val deviceId = "device1"
    val key = experimentService.keyGen(Some(deviceId), Some(userId), None, None)
    val fields = experimentService.getFieldsMap(Some(deviceId), Some(userId), None, None)

    val databaseIndex: Int = config.getInt("redis.experimentIndex")
    val jedis = redisUtil.getConnection(databaseIndex);
    jedis.set(key, Constants.EXPERIMENT_DATA);
    jedis.close();

    val result = Await.result(experimentService.getExperiment(Some(deviceId), Some(userId), None, None), 20.seconds)

    result.get.userId should be("user1")
    result.get.key should be("325324123413")
    result.get.id should be("exp1")
    result.get.name should be("first-exp")

    // should not call elasticsearch when data is present in redis
    verify(elasticsearchServiceMock, timeout(1000).times(0)).searchExperiment(fields)

  }

  it should "resolve default experiment if not defined" in {
    val experimentData = ExperimentData(id = "exp1", name = "experiment1", startDate = "2019-11-21",
      endDate = "2019-11-22", key = "", expType = "", userId = "", deviceId = "", userIdMod = 0, deviceIdMod = 0)
    val result = experimentService.resolveExperiment(experimentData)
    result.getOrElse(None) should be (experimentData)
  }
}