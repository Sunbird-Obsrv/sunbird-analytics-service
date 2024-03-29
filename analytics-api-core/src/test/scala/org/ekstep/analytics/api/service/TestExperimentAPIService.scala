package org.ekstep.analytics.api.service

import org.ekstep.analytics.api._
import org.joda.time.DateTime
import akka.actor.ActorSystem
import akka.testkit.TestActorRef
import com.typesafe.config.ConfigFactory
import org.ekstep.analytics.api.service.ExperimentAPIService.CreateExperimentRequest
import akka.actor.ActorRef
import org.ekstep.analytics.api.service.ExperimentAPIService.GetExperimentRequest
import org.ekstep.analytics.api.util.{EmbeddedPostgresql, ExperimentDefinition, JSONUtils, PostgresDBUtil}

class TestExperimentAPIService extends BaseSpec {

    var postgresUtil: PostgresDBUtil = null
    override def beforeAll(): Unit = {
        super.beforeAll()
        EmbeddedPostgresql.start()
        EmbeddedPostgresql.createTables()
        postgresUtil = new PostgresDBUtil
    }

    override def afterAll(): Unit = {
        super.afterAll()
        EmbeddedPostgresql.close()
    }

    implicit val actorSystem: ActorSystem = ActorSystem("testActorSystem", config)
//    private val postgresUtil = new PostgresDBUtil
    val experimentServiceActorRef = TestActorRef(new ExperimentAPIService(postgresUtil))

    val startDate: String = DateTime.now().toString("yyyy-MM-dd")
    val endDate: String = DateTime.now().plusDays(10).toString("yyyy-MM-dd")

    "ExperimentAPIService" should "return response for data request" in {

        // resubmit for failed
        val req = Array(ExperimentDefinition("UR1235", "test_exp", "Test Exp", "Test", "Test1", Option(DateTime.now), Option(DateTime.now),
          "", "", Option("Failed"), Option(""), Option("""{"one":1}""")))
        postgresUtil.saveExperimentDefinition(req)
        val request2 = s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1235","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"$startDate","endDate":"$endDate","key":"/org/profile","client":"portal","modulus":5}}}"""
        val resp = ExperimentAPIService.createRequest(request2, postgresUtil)
        resp.responseCode should be("OK")
        resp.result.get.get("status") should be (Some("SUBMITTED"))
        resp.result.get.get("status_msg") should be (Some("Experiment successfully submitted"))

        // already exist check
        val request = s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"$startDate","endDate":"$endDate","key":"/org/profile","client":"portal","modulus":5}}}"""
        val response = ExperimentAPIService.createRequest(request, postgresUtil)
        response.responseCode should be("OK")

        val resp2 = ExperimentAPIService.createRequest(request, postgresUtil)
        resp2.responseCode should be("OK")
        resp2.result.get.get("err") should be (Some("failed"))
        resp2.result.get.get("errorMsg") should be (Some(Map("msg" -> "ExperimentId already exists.")))
        
        
    }

    it should "return error response for data request" in {
        val request = s"""{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"organisations.orgName":["sunbird"]}},"data":{"startDate":"$startDate","endDate":"$endDate","key":"/org/profile","client":"portal"}}}"""
        val response = ExperimentAPIService.createRequest(request, postgresUtil)
        response.responseCode should be("CLIENT_ERROR")
    }
    
    it should "return error response with all validation errors for data request" in {
        val request = """{"id":"ekstep.analytics.dataset.request.submit","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{}}"""
        val response = ExperimentAPIService.createRequest(request, postgresUtil)
        response.params.errorMsg should be(Map("status" -> "failed", "request.createdBy" -> "Criteria should not be empty", "request.expid" -> "Experiment Id should not be  empty", "request.data" -> "Experiment Data should not be empty", "request.name" -> "Experiment Name should not be  empty"))
    }

    it should "return the experiment for experimentid" in {
        val req = Array(ExperimentDefinition("UR12356", "test_exp", "Test Exp", "Test", "Test1", Option(DateTime.now), Option(DateTime.now),
          """{"type":"user"}""", """{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}""", Option("Failed"), Option(""), Option("""{"one":1}""")))
        postgresUtil.saveExperimentDefinition(req)
        val response = ExperimentAPIService.getExperimentDefinition("UR12356", postgresUtil)
        response.responseCode should be("OK")
    }

    it should "return the error for no experimentid" in {
        val response = ExperimentAPIService.getExperimentDefinition("H1234", postgresUtil)
        response.params.errmsg  should be ("no experiment available with the given experimentid")
    }
    
    it should "test the exception branches" in {
      var resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request" -> "Request should not be empty"))
      
      resp = ExperimentAPIService.createRequest(s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user"},"data":{"startDate":"$startDate","endDate":"$endDate","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request.filters" -> "Criteria Filters should not be empty"))
      
      resp = ExperimentAPIService.createRequest(s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"filters":{"emailVerified":true}},"data":{"startDate":"$startDate","endDate":"$endDate","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "request.type" -> "Criteria Type should not be empty"))
      
      resp = ExperimentAPIService.createRequest(s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"$startDate","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.endDate" -> "Experiment End_Date should not be empty"))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2019-08-21","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.endDate" -> "End_Date should be greater than today's date."))
      
      resp = ExperimentAPIService.createRequest(s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"endDate":"$startDate","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Experiment Start_Date should not be empty"))
      
      resp = ExperimentAPIService.createRequest(s"""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2019-08-09","endDate":"$endDate","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Start_Date should be greater than or equal to today's date.."))
      
      resp = ExperimentAPIService.createRequest("""{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2041-08-09","endDate":"2040-08-21","key":"/org/profile","client":"portal","modulus":5}}}""", postgresUtil)
      resp.responseCode should be("CLIENT_ERROR")
      resp.params.errorMsg should be (Map("status" -> "failed", "data.startDate" -> "Date range should not be -ve. Please check your start_date & end_date"))
      
      val request = """{"id":"ekstep.analytics.experiment.create","ver":"1.0","ts":"2016-12-07T12:40:40+05:30","params":{"msgid":"4f04da60-1e24-4d31-aa7b-1daf91c46341","client_key":"dev-portal"},"request":{"expId":"UR1234","name":"USER_ORG","createdBy":"User1","description":"Experiment to get users to explore page ","criteria":{"type":"user","filters":{"emailVerified":true}},"data":{"startDate":"2021-08-09","endDate":"2021-08-21","key":"/org/profile","client":"portal","modulus":5}}}"""
      experimentServiceActorRef.tell(CreateExperimentRequest(request, config), ActorRef.noSender)
      experimentServiceActorRef.tell(GetExperimentRequest(request, config), ActorRef.noSender)
    }

}