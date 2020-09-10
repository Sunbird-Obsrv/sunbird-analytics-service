package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.{BaseSpec, ExperimentDefinition}
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime

class TestDBUtil extends BaseSpec {

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