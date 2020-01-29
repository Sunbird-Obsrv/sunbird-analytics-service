package org.ekstep.analytics.api.service

import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import net.manub.embeddedkafka.EmbeddedKafka
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.util.KafkaUtil
import akka.testkit.TestActorRef
import org.ekstep.analytics.api.util.AppConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.ekstep.analytics.api.util.JSONUtils

class TestSaveMetricsActor extends FlatSpec with Matchers with BeforeAndAfterAll with MockFactory with EmbeddedKafka {
  
  implicit val config = ConfigFactory.load()
  private implicit val system: ActorSystem = ActorSystem("savemetrics-test-actor-system", config)
  private val kafkaUtil = new KafkaUtil();
  val saveMetricsActor = TestActorRef(new SaveMetricsActor(kafkaUtil))
  implicit val serializer = new StringSerializer()
  implicit val deserializer = new StringDeserializer()
  
  "SaveMetricsActor" should "assert for all the methods" in {

    kafkaUtil.close(); // Added this line to get 100% coverage for KafkaUtil
    val userDefinedConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
    withRunningKafkaOnFoundPort(userDefinedConfig) { implicit actualConfig =>
      saveMetricsActor.receive(IncrementApiCalls)
      saveMetricsActor.receive(IncrementApiCalls)
      saveMetricsActor.receive(IncrementApiCalls)
      saveMetricsActor.receive(IncrementApiCalls)
      saveMetricsActor.receive(IncrementLocationDbHitCount)
      saveMetricsActor.receive(IncrementLocationDbHitCount)
      saveMetricsActor.receive(IncrementLocationDbHitCount)
      saveMetricsActor.receive(IncrementLocationDbMissCount)
      saveMetricsActor.receive(IncrementLocationDbMissCount)
      saveMetricsActor.receive(IncrementLocationDbSuccessCount)
      saveMetricsActor.receive(IncrementLocationDbSuccessCount)
      saveMetricsActor.receive(IncrementLocationDbSuccessCount)
      saveMetricsActor.receive(IncrementLocationDbErrorCount)
      saveMetricsActor.receive(IncrementLogDeviceRegisterSuccessCount)
      
      saveMetricsActor.receive(SaveMetrics)
      
      val counts = saveMetricsActor.underlyingActor.getCounts();
      counts._1 should be (0)
      counts._2 should be (0)
      counts._3 should be (0)
      counts._4 should be (0)
      counts._5 should be (0)
      counts._6 should be (0)
      
      val topic = AppConfig.getString("kafka.metrics.event.topic");
      val msg = consumeFirstMessageFrom(topic);
      msg should not be (null);
      val map = JSONUtils.deserialize[Map[String, AnyRef]](msg);
      
      Console.println("map", map);
      map.get("location-db-hit-count").get should be (3)
      map.get("log-device-register-success-count").get should be (1)
      map.get("location-db-miss-count").get should be (2)
      map.get("api-calls").get should be (4)
      map.get("location-db-success-count").get should be (3)
      map.get("location-db-error-count").get should be (1)
      
    }
  }
  
}