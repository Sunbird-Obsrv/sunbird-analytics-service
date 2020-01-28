package org.ekstep.analytics.api.util

import com.sksamuel.elastic4s.http.HttpClient
import org.ekstep.analytics.api.BaseSpec
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.collection.mutable.Buffer
import org.ekstep.analytics.api.service.experiment.ExperimentData


class TestElasticsearchService extends FlatSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val httpClientMock = mock[HttpClient]
  val ESservice = new ElasticsearchService()
  implicit val executor =  scala.concurrent.ExecutionContext.global

  "ElasticsearchService" should "search return None if ES connection doesn't exist" in {

    val response = ESservice.searchExperiment(Map("deviceId" -> "device3", "userId" -> "user3", "url" -> "http://xyz.com", "producer"-> "sunbird.app"))
    val result = Await.result(response, 5.seconds)
    result should be (None);
    
    ESservice.healthCheck should be (false);
  }
  
  it should "valid experiment data" in {
    val expMapping = """{"experiment":{"properties":{"id":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"name":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"startDate":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"endDate":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"key":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"expType":{"type":"text","fields":{"raw":{"type":"text","fielddata":true}}},"userId":{"type":"keyword"},"userIdMod":{"type":"long"},"deviceId":{"type":"keyword"},"deviceIdMod":{"type":"long"},"url":{"type":"keyword"}}}}""";
    val searchExperimentIndex = AppConfig.getString("elasticsearch.searchExperiment.index")
    EmbeddedES.start(Array(EsIndex(searchExperimentIndex, Option(searchExperimentIndex), Option(expMapping), None)))
    EmbeddedES.loadData(searchExperimentIndex, searchExperimentIndex, Buffer(
      """{"id":"Exp1","name":"Experiment 1","startDate":"2020-01-01","endDate":"2020-01-31","key":"key1","expType":"test","userId":"user1"}""",
      """{"id":"Exp2","name":"Experiment 2","startDate":"2020-01-01","endDate":"2020-01-31","key":"key2","expType":"test","deviceId":"device1"}""",
      """{"id":"Exp3","name":"Experiment 3","startDate":"2020-01-01","endDate":"2020-01-31","key":"key3","expType":"test","url":"http://xyz.com"}""",
      """{"id":"Exp4","name":"Experiment 4","startDate":"2020-01-01","endDate":"2020-01-31","key":"key4","expType":"test","userId":"user1","deviceId":"device2"}""",
      """{"id":"Exp5","name":"Experiment 5","startDate":"2020-01-01","endDate":"2020-01-31","key":"key5","expType":"test","userId":"user1","deviceId":"device1","url":"http://xyz.com"}"""
    ))
    
    ESservice.healthCheck should be (true);
    
    var response = ESservice.searchExperiment(Map("userId" -> "user1"))
    var result = Await.result(response, 5.seconds)
    result.get should be (ExperimentData("Exp1", "Experiment 1", "2020-01-01", "2020-01-31", "key1","test","user1",null,0,0));
    
    response = ESservice.searchExperiment(Map("deviceId" -> "device1"))
    result = Await.result(response, 5.seconds)
    result.get should be (ExperimentData("Exp2", "Experiment 2", "2020-01-01", "2020-01-31", "key2","test",null,"device1",0,0));
    
    response = ESservice.searchExperiment(Map("deviceId" -> "device2", "url" -> "http://xyz.com"))
    result = Await.result(response, 5.seconds)
    result.get should be (ExperimentData("Exp3", "Experiment 3", "2020-01-01", "2020-01-31", "key3","test",null,null,0,0));
    
    response = ESservice.searchExperiment(Map("userId" -> "user1", "deviceId" -> "device2"))
    result = Await.result(response, 5.seconds)
    result.get should be (ExperimentData("Exp4", "Experiment 4", "2020-01-01", "2020-01-31", "key4","test","user1","device2",0,0));
    
    response = ESservice.searchExperiment(Map("userId" -> "user1", "deviceId" -> "device1", "url" -> "http://xyz.com"))
    result = Await.result(response, 5.seconds)
    result.get should be (ExperimentData("Exp5", "Experiment 5", "2020-01-01", "2020-01-31", "key5","test","user1","device1",0,0));
    
    response = ESservice.searchExperiment(Map("userId" -> "user2"))
    result = Await.result(response, 5.seconds)
    result should be (None);
    
    EmbeddedES.esServer.deleteIndices();
    
    response = ESservice.searchExperiment(Map("userId" -> "user1"))
    result = Await.result(response, 5.seconds)
    result should be (None);
    
    ESservice.healthCheck should be (false);
    
    EmbeddedES.stop();
  }
  
}
