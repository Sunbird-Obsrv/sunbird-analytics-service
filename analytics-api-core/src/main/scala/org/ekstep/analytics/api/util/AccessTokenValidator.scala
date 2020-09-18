package org.ekstep.analytics.api.util

import java.nio.charset.StandardCharsets
import java.util.Base64

import javax.inject.Singleton
import org.ekstep.analytics.framework.conf.AppConf

@Singleton
class AccessTokenValidator {

    implicit val className = "org.ekstep.analytics.api.util.auth_verifier.AccessTokenValidator"

    def getUserId(token: String): String = {
        var userId = JsonKey.UNAUTHORIZED
        val tokenElements = token.split("\\.")
        val body = tokenElements(1)
        val payload = JSONUtils.deserialize[Map[String, AnyRef]](new String(decodeFromBase64(body)))
        if (payload.nonEmpty && checkIss(payload.getOrElse("iss", "").asInstanceOf[String])) {
            userId = payload.getOrElse(JsonKey.SUB, "").asInstanceOf[String]
            if (userId.nonEmpty) {
                val pos = userId.lastIndexOf(":")
                userId = userId.substring(pos + 1)
            }
        }
        userId
    }

    private def checkIss(iss: String) = {
        val realmUrl = AppConf.getConfig(JsonKey.SSO_URL) + "realms/" + AppConf.getConfig(JsonKey.SSO_REALM)
        realmUrl.equalsIgnoreCase(iss)
    }

    def decodeFromBase64(data: String): Array[Byte] = {
        Base64.getMimeDecoder.decode(data.getBytes(StandardCharsets.UTF_8))
    }

}

object JsonKey {
    val UNAUTHORIZED = "Unauthorized"
    val SUB = "sub"
    val SSO_URL = "sso.url"
    val SSO_REALM = "sso.realm"
}
