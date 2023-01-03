package org.ekstep.analytics.api.util

import javax.inject.Singleton
import org.ekstep.analytics.framework.util.{HTTPClient, RestUtil}

@Singleton
class APIRestUtil {
    def get[T](apiURL: String, headers: Option[Map[String,String]] = None, restUtil: HTTPClient = RestUtil)(implicit mf: Manifest[T]): T = {
        restUtil.get[T](apiURL, headers)
    }

    def post[T](apiURL: String, body: String, headers: Option[Map[String,String]] = None, restUtil: HTTPClient = RestUtil)(implicit mf: Manifest[T]): T = {
        restUtil.post[T](apiURL, body, headers)
    }
}
