package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.{BaseSpec, ExperimentDefinition, JobRequest}
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime

class TestDBUtil extends BaseSpec {

  it should "fetch list of jobs in a descending order" in {

    val res1 = CassandraUtil.session.execute("DELETE FROM " + AppConf.getConfig("application.env") + "_platform_db.job_request WHERE client_key='partner1'")
    val request_data1 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["becb887fe82f24c644482eb30041da6d88bd8150"]}}"""
    val request_data2 = """{"filter":{"start_date":"2016-11-19","end_date":"2016-11-20","tags":["test-tag"],"events":["OE_ASSESS"]}}"""

    val requests = Array(
      JobRequest(Option("partner1"), Option("1234"), None, Option("SUBMITTED"), Option(request_data1),
        Option(1), Option(DateTime.now()), None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
      JobRequest(Option("partner1"), Option("273645"), Option("test-job-id"), Option("COMPLETED"), Option(request_data2),
        Option(1), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), Option("https://test-location"), Option(DateTime.parse("2017-01-08", CommonUtil.dateFormat)), None, None, None, None, None, Option(123234), Option(532), Option(12343453L), None, None, None, None, None))
    CassandraUtil.saveJobRequest(requests)

    val jobs = CassandraUtil.getJobRequestList("partner1")

    jobs.last.status.get should be("COMPLETED")
    jobs.head.status.get should be("SUBMITTED")
  }


  it should "able to query the experiment def data" in {
    val request = Array(ExperimentDefinition("exp_01", "test_exp", "Test Exp", "Test", "Test1", Option(DateTime.now), Option(DateTime.now),
      "", "", Option("Active"), Option(""), Option(Map("one" -> 1L))))
    CassandraUtil.saveExperimentDefinition(request)
    CassandraUtil.session.execute("SELECT * FROM " + AppConf.getConfig("application.env") + "_platform_db.experiment_definition")
    val result = CassandraUtil.getExperimentDefinition("exp_01")
    result.get.expName should be("test_exp")
    
    val request2 = Array(ExperimentDefinition("exp_02", "test_exp2", "Test Exp", "Test", "Test1", None, Option(DateTime.now),
      "", "", Option("Active"), Option(""), Option(Map("one" -> 1L))))
    CassandraUtil.saveExperimentDefinition(request2)
    CassandraUtil.session.execute("SELECT * FROM " + AppConf.getConfig("application.env") + "_platform_db.experiment_definition")
    val result2 = CassandraUtil.getExperimentDefinition("exp_02")
    result2.get.expName should be("test_exp2")
    
    CassandraUtil.session.close();
    CassandraUtil.checkCassandraConnection() should be (false);
    
    CassandraUtil.session = CassandraUtil.cluster.connect();
  }

}