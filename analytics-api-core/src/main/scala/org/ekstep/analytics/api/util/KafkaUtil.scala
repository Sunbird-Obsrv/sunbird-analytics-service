package org.ekstep.analytics.api.util

import javax.inject._
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.HashMap
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

@Singleton
class KafkaUtil {

  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000L.asInstanceOf[Object]);
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.getString("kafka.broker.list"));
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  private var producer: KafkaProducer[String, String] = _;

  def send(event: String, topic: String) = {
    if(null == producer) producer = new KafkaProducer[String, String](props);
    val message = new ProducerRecord[String, String](topic, null, event);
    producer.send(message);
  }
  
  def close() {
    if(null != producer)
      producer.close();
  }
}