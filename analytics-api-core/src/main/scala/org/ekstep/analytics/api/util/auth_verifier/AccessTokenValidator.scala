package org.ekstep.analytics.api.util.auth_verifier

import java.nio.charset.StandardCharsets
import com.fasterxml.jackson.core.JsonProcessingException
import org.ekstep.analytics.api.util.{APILogger, JSONUtils}
import java.util.Base64

import javax.inject.Singleton
import org.ekstep.analytics.framework.conf.AppConf

@Singleton
class AccessTokenValidator {

    implicit val className = "org.ekstep.analytics.api.util.auth_verifier.AccessTokenValidator"

    def verifyUserToken(token: String, checkExpiry: Boolean = true, keyManager: KeyManager = new KeyManager, cryptoUtil: CryptoUtil = new CryptoUtil): String = {
        var userId = JsonKey.UNAUTHORIZED
        try {
            val payload = validateToken(token, checkExpiry, keyManager, cryptoUtil)
            println("payload: " + payload)
            if (payload.nonEmpty && checkIss(payload.getOrElse("iss", "").asInstanceOf[String])) {
                userId = payload.getOrElse(JsonKey.SUB, "").asInstanceOf[String]
                if (userId.nonEmpty) {
                    val pos = userId.lastIndexOf(":")
                    userId = userId.substring(pos + 1)
                }
            }
        } catch {
            case ex: Exception =>
                ex.printStackTrace()
                println("Exception in verifyUserAccessToken: verify: " + ex)
                APILogger.log("Exception in verifyUserAccessToken: verify: " + ex)
        }
        userId
    }

    @throws[JsonProcessingException]
    def validateToken(token: String, checkExpiry: Boolean = true, keyManager: KeyManager, cryptoUtil: CryptoUtil): Map[String, Object] = {
        val tokenElements = token.split("\\.")
        val header = tokenElements(0)
        val body = tokenElements(1)
        val signature = tokenElements(2)
        val payLoad = header + JsonKey.DOT_SEPARATOR + body
        val headerData = JSONUtils.deserialize[Map[String, AnyRef]](new String(decodeFromBase64(header)))
        val keyId = headerData.getOrElse("kid", "").asInstanceOf[String]
        println(headerData, keyId, JsonKey.SHA_256_WITH_RSA, decodeFromBase64(signature))
        val isValid = cryptoUtil.verifyRSASign(payLoad, decodeFromBase64(signature), keyManager.getPublicKey(keyId).publicKey, JsonKey.SHA_256_WITH_RSA)
        println("isValid: " + isValid)
        if (isValid) {
            val tokenBody = JSONUtils.deserialize[Map[String, AnyRef]](new String(decodeFromBase64(body)))
            println("tokenBody: " + tokenBody)
            if (checkExpiry) {
                val isExp = isExpired(tokenBody.getOrElse("exp", 0).asInstanceOf[Integer])
                if (isExp) return Map.empty
            }
            else
                return tokenBody
        }
        Map.empty
    }

    private def checkIss(iss: String) = {
        val realmUrl = AppConf.getConfig(JsonKey.SSO_URL) + "realms/" + AppConf.getConfig(JsonKey.SSO_REALM)
        realmUrl.equalsIgnoreCase(iss)
    }

    private def decodeFromBase64(data: String): Array[Byte] = {
        Base64.getMimeDecoder.decode(data.getBytes(StandardCharsets.UTF_8))
    }

    private def isExpired(expiration: Int): Boolean = {
        return (System.currentTimeMillis()/1000 > expiration)
    }

}

object JsonKey {
    val UNAUTHORIZED = "Unauthorized"
    val SUB = "sub"
    val DOT_SEPARATOR = "."
    val SHA_256_WITH_RSA = "SHA256withRSA"
    val ACCESS_TOKEN_PUBLICKEY_BASEPATH = "accesstoken.publickey.basepath"
    val SSO_URL = "sso.url"
    val SSO_REALM = "sso.realm"
}