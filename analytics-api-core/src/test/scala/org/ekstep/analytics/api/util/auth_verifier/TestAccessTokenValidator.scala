package org.ekstep.analytics.api.util.auth_verifier

import java.nio.charset.StandardCharsets
import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, PublicKey}
import java.util.Base64

import org.ekstep.analytics.api.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{reset, when}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class TestAccessTokenValidator extends FlatSpec with Matchers with MockitoSugar {

    val KeyManagerMock = mock[KeyManager]
    val cryptoUtilMock = mock[CryptoUtil]
    val accessTokenValidator = new AccessTokenValidator()
    val token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5emhhVnZDbl81OEtheHpldHBzYXNZQ2lEallkemJIX3U2LV93SDk4SEc0In0.eyJqdGkiOiI5ZmQzNzgzYy01YjZmLTQ3OWQtYmMzYy0yZWEzOGUzZmRmYzgiLCJleHAiOjE1MDUxMTQyNDYsIm5iZiI6MCwiaWF0IjoxNTA1MTEzNjQ2LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoic2VjdXJpdHktYWRtaW4tY29uc29sZSIsInN1YiI6ImIzYTZkMTY4LWJjZmQtNDE2MS1hYzVmLTljZjYyODIyNzlmMyIsInR5cCI6IkJlYXJlciIsImF6cCI6InNlY3VyaXR5LWFkbWluLWNvbnNvbGUiLCJub25jZSI6ImMxOGVlMDM2LTAyMWItNGVlZC04NWVhLTc0MjMyYzg2ZmI4ZSIsImF1dGhfdGltZSI6MTUwNTExMzY0Niwic2Vzc2lvbl9zdGF0ZSI6ImRiZTU2NDlmLTY4MDktNDA3NS05Njk5LTVhYjIyNWMwZTkyMiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOltdLCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiTWFuemFydWwgaGFxdWUiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MTIzNDU2NyIsImdpdmVuX25hbWUiOiJNYW56YXJ1bCBoYXF1ZSIsImVtYWlsIjoidGVzdDEyM0B0LmNvbSJ9.Xdjqe16MSkiR94g-Uj_pVZ2L3gnIdKpkJ6aB82W_w_c3yEmx1mXYBdkxe4zMz3ks4OX_PWwSFEbJECHcnujUwF6Ula0xtXTfuESB9hFyiWHtVAhuh5UlCCwPnsihv5EqK6u-Qzo0aa6qZOiQK3Zo7FLpnPUDxn4yHyo3mRZUiWf76KTl8PhSMoXoWxcR2vGW0b-cPixILTZPV0xXUZoozCui70QnvTgOJDWqr7y80EWDkS4Ptn-QM3q2nJlw63mZreOG3XTdraOlcKIP5vFK992dyyHlYGqWVzigortS9Ah4cprFVuLlX8mu1cQvqHBtW-0Dq_JlcTMaztEnqvJ6XA"
    val keyId = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkYnB+jSS4oCJVzTczkWBjCWgxAOjhB+/2HRnRr2PV457R1YxgV9Krh1CsqvWnXdKF8id1vOx7NCf7cUHOil6THZjwMLv3g/9IAzDDBKCGaoY1X+dAPs93CQxswDBDWjFBuZJi/nJ2b1PHNX4ErZmjqTXqUMMEIW5GKFbVKficXrX7FuSMoQ3se7daLXC4oZcw7nBeINGj6Aitr2W2tPjGkecgbhNxGO6KRMPex74IwF7IZ2zwisLNYOH7C03F/lU+8c2g6gcSMto3CYF7Xj4Nk2rzbn2hLdJ3d/Eh5OqnIyZ8L8/V9ini5kSp4bonILvJ67uifud7AbmwcdN6sD5MwIDAQAB"
    val publicKey = KeyManagerMock.loadPublicKey(keyId)
    when(KeyManagerMock.getPublicKey(ArgumentMatchers.any())).thenReturn(KeyData(keyId, publicKey))

    "AccessTokenValidator" should "validate token and return valid user id" in {

        when(cryptoUtilMock.verifyRSASign(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true)
        val userId = accessTokenValidator.verifyUserToken(token, false, KeyManagerMock, cryptoUtilMock)
        userId should not be("Unauthorized")
        userId should be("b3a6d168-bcfd-4161-ac5f-9cf6282279f3")
    }

    "AccessTokenValidator" should "validation for invalid token" in {

        when(cryptoUtilMock.verifyRSASign(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(false)
        val userId = accessTokenValidator.verifyUserToken(token, false, KeyManagerMock, cryptoUtilMock)
        userId should be("Unauthorized")
    }

    "AccessTokenValidator" should "validation for expired token" in {

        when(cryptoUtilMock.verifyRSASign(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(true)
        val userId = accessTokenValidator.verifyUserToken(token, true, KeyManagerMock, cryptoUtilMock)
        userId should be("Unauthorized")
    }

    "AccessTokenValidator" should "cover all cases" in {

        val tokenElements = token.split("\\.")
        val header = tokenElements(0)
        val body = tokenElements(1)
        val signature = tokenElements(2)
        val payLoad = header + JsonKey.DOT_SEPARATOR + body
        val headerData = JSONUtils.deserialize[Map[String, AnyRef]](new String(accessTokenValidator.decodeFromBase64(header)))
        val keyId = headerData.getOrElse("kid", "").asInstanceOf[String]
        val isValid = cryptoUtilMock.verifyRSASign(payLoad, accessTokenValidator.decodeFromBase64(signature), KeyManagerMock.getPublicKey(keyId).publicKey, JsonKey.SHA_256_WITH_RSA)
        isValid should be(true)

        reset(cryptoUtilMock)
        val userId = accessTokenValidator.verifyUserToken(token, true, KeyManagerMock, cryptoUtilMock)
        userId should be("Unauthorized")
    }


}
