package org.ekstep.analytics.api.util.auth_verifier

import org.ekstep.analytics.api.util.JSONUtils
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class TestKeyManager extends FlatSpec with Matchers with MockitoSugar {

    val keyManager = new KeyManager()
    keyManager.init()

    "KeyManager" should "load and get public key" in {

        val keyId = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkYnB+jSS4oCJVzTczkWBjCWgxAOjhB+/2HRnRr2PV457R1YxgV9Krh1CsqvWnXdKF8id1vOx7NCf7cUHOil6THZjwMLv3g/9IAzDDBKCGaoY1X+dAPs93CQxswDBDWjFBuZJi/nJ2b1PHNX4ErZmjqTXqUMMEIW5GKFbVKficXrX7FuSMoQ3se7daLXC4oZcw7nBeINGj6Aitr2W2tPjGkecgbhNxGO6KRMPex74IwF7IZ2zwisLNYOH7C03F/lU+8c2g6gcSMto3CYF7Xj4Nk2rzbn2hLdJ3d/Eh5OqnIyZ8L8/V9ini5kSp4bonILvJ67uifud7AbmwcdN6sD5MwIDAQAB"
        val publicKey = keyManager.loadPublicKey(keyId)
        (publicKey.toString.length) > 0 should be (true)

        val keyData = keyManager.getPublicKey(keyId)
        keyData should be(null)
    }

    "CryptoUtil" should "test all cases" in {

        val cryptoUtil = new CryptoUtil()
        val accessTokenValidator = new AccessTokenValidator()

        val keyId = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkYnB+jSS4oCJVzTczkWBjCWgxAOjhB+/2HRnRr2PV457R1YxgV9Krh1CsqvWnXdKF8id1vOx7NCf7cUHOil6THZjwMLv3g/9IAzDDBKCGaoY1X+dAPs93CQxswDBDWjFBuZJi/nJ2b1PHNX4ErZmjqTXqUMMEIW5GKFbVKficXrX7FuSMoQ3se7daLXC4oZcw7nBeINGj6Aitr2W2tPjGkecgbhNxGO6KRMPex74IwF7IZ2zwisLNYOH7C03F/lU+8c2g6gcSMto3CYF7Xj4Nk2rzbn2hLdJ3d/Eh5OqnIyZ8L8/V9ini5kSp4bonILvJ67uifud7AbmwcdN6sD5MwIDAQAB"
        val publicKey = keyManager.loadPublicKey(keyId)
        val token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI5emhhVnZDbl81OEtheHpldHBzYXNZQ2lEallkemJIX3U2LV93SDk4SEc0In0.eyJqdGkiOiI5ZmQzNzgzYy01YjZmLTQ3OWQtYmMzYy0yZWEzOGUzZmRmYzgiLCJleHAiOjE1MDUxMTQyNDYsIm5iZiI6MCwiaWF0IjoxNTA1MTEzNjQ2LCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwODAvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoic2VjdXJpdHktYWRtaW4tY29uc29sZSIsInN1YiI6ImIzYTZkMTY4LWJjZmQtNDE2MS1hYzVmLTljZjYyODIyNzlmMyIsInR5cCI6IkJlYXJlciIsImF6cCI6InNlY3VyaXR5LWFkbWluLWNvbnNvbGUiLCJub25jZSI6ImMxOGVlMDM2LTAyMWItNGVlZC04NWVhLTc0MjMyYzg2ZmI4ZSIsImF1dGhfdGltZSI6MTUwNTExMzY0Niwic2Vzc2lvbl9zdGF0ZSI6ImRiZTU2NDlmLTY4MDktNDA3NS05Njk5LTVhYjIyNWMwZTkyMiIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOltdLCJyZXNvdXJjZV9hY2Nlc3MiOnt9LCJuYW1lIjoiTWFuemFydWwgaGFxdWUiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MTIzNDU2NyIsImdpdmVuX25hbWUiOiJNYW56YXJ1bCBoYXF1ZSIsImVtYWlsIjoidGVzdDEyM0B0LmNvbSJ9.Xdjqe16MSkiR94g-Uj_pVZ2L3gnIdKpkJ6aB82W_w_c3yEmx1mXYBdkxe4zMz3ks4OX_PWwSFEbJECHcnujUwF6Ula0xtXTfuESB9hFyiWHtVAhuh5UlCCwPnsihv5EqK6u-Qzo0aa6qZOiQK3Zo7FLpnPUDxn4yHyo3mRZUiWf76KTl8PhSMoXoWxcR2vGW0b-cPixILTZPV0xXUZoozCui70QnvTgOJDWqr7y80EWDkS4Ptn-QM3q2nJlw63mZreOG3XTdraOlcKIP5vFK992dyyHlYGqWVzigortS9Ah4cprFVuLlX8mu1cQvqHBtW-0Dq_JlcTMaztEnqvJ6XA"
        val tokenElements = token.split("\\.")
        val header = tokenElements(0)
        val body = tokenElements(1)
        val signature = tokenElements(2)
        val payLoad = header + JsonKey.DOT_SEPARATOR + body
        val isValid1 = cryptoUtil.verifyRSASign(payLoad, accessTokenValidator.decodeFromBase64(signature), publicKey, JsonKey.SHA_256_WITH_RSA)
        isValid1 should be(false)

        // exception case
        val isValid2 = cryptoUtil.verifyRSASign("", accessTokenValidator.decodeFromBase64(""), publicKey, JsonKey.SHA_256_WITH_RSA)
        isValid2 should be(false)
    }
}
