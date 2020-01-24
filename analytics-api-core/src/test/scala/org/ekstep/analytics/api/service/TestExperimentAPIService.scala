package org.ekstep.analytics.api.service

import org.ekstep.analytics.api._
import org.ekstep.analytics.api.util.CassandraUtil
import org.joda.time.DateTime
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.service.ExperimentAPIService.CreateExperimentRequest
import akka.actor.ActorRef
import org.ekstep.analytics.api.service.ExperimentAPIService.GetExperimentRequest

class TestExperimentAPIService extends BaseSpec {
  
    implicit val actorSystem: ActorSystem = ActorSystem("testActorSystem", config)
    val experimentServiceActorRef = TestActorRef(new ExperimentAPIService)

    "ExperimentAPIService" should "return response for data request" in {
        val request = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}"""
        val response = ExperimentAPIService.createRequest(request)
        response.responseCode should be("OK")
        
        val req = Array(ExperimentDefinition("UR1235", "test_exp", "Test Exp", "Test", "Test1", Option(DateTime.now), Option(DateTime.now),
          "", "", Option("Failed"), Option(""), Option(Map("one" -> 1L))))
        CassandraUtil.saveExperimentDefinition(req)
        val request2 = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1235","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}"""
        val resp = ExperimentAPIService.createRequest(request2)
        Console.println("resp", resp);
        resp.responseCode should be("OK")
        Console.println("resp.result", resp.result);
        resp.result.get.get("status") should be (Some("SUBMITTED"))
        resp.result.get.get("status_msg") should be (Some("Experiment successfully submitted"))
        
        val resp2 = ExperimentAPIService.createRequest(request)
        Console.println("resp2", resp2);
        resp2.responseCode should be("OK")
        resp2.result.get.get("err") should be (Some("failed"))
        resp2.result.get.get("errorMsg") should be (Some(Map("msg" -> "ExperimentId already exists.")))
        
        
    }

    it should "return error response for data request" in {
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2021-08-01","endDate":"2021-08-02","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.createRequest(request)
        response.responseCode should be("CLIENT_ERROR")
    }
    
    it should "return error response with all validation errors for data request" in {
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{}}"""
        val response = ExperimentAPIService.createRequest(request)
        response.params.errorMsg should be(Map("status" -> "failed", "request.createdBy" -> "Criteria should not be empty", "request.expid" -> "Experiment Id should not be  empty", "request.data" -> "Experiment Data should not be empty", "request.name" -> "Experiment Name should not be  empty"))
    }

    it should "return the experiment for experimentid" in {
        val req = Array(ExperimentDefinition("UR1235", "test_exp", "Test Exp", "Test", "Test1", Option(DateTime.now), Option(DateTime.now),
          """{"type":"user"}"""", """{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}""", Option("Failed"), Option(""), Option(Map("one" -> 1L))))
        CassandraUtil.saveExperimentDefinition(req)
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"2022-08-01","endDate":"2022-08-02","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.getExperimentDefinition("UR1235")
        response.responseCode should be("OK")
    }

    it should "return the error for no experimentid" in {
        val response = ExperimentAPIService.getExperimentDefinition("H1234")
        response.params.errmsg  should be ("no experiment available with the given experimentid")
    }
    
    it should "test the exception branches" in {
      var resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request" -> "Request should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user"},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request.filters" -> "Criteria Filters should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request.type" -> "Criteria Type should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.endDate" -> "Experiment End_Date should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2019-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.endDate" -> "End_Date should be greater than today's date."))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Experiment Start_Date should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2019-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Start_Date should be greater than or equal to today's date.."))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2020-08-21","key":"/org/profile","client":"portal","modulus":5}}}""")
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Date range should not be -ve. Please check your start_date & end_date"))
      
      val request = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}"""
      experimentServiceActorRef.tell(CreateExperimentRequest(request, config), ActorRef.noSender)
      experimentServiceActorRef.tell(GetExperimentRequest(request, config), ActorRef.noSender)
    }

}