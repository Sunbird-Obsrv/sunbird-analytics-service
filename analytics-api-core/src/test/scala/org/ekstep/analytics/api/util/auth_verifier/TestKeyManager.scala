package org.ekstep.analytics.api.util.auth_verifier

import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar

class TestKeyManager extends FlatSpec with Matchers with MockitoSugar {

    "KeyManager" should "validate token and return valid user id" in {

        val keyManager = new KeyManager()
        keyManager.init()
        val keyId = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkYnB+jSS4oCJVzTczkWBjCWgxAOjhB+/2HRnRr2PV457R1YxgV9Krh1CsqvWnXdKF8id1vOx7NCf7cUHOil6THZjwMLv3g/9IAzDDBKCGaoY1X+dAPs93CQxswDBDWjFBuZJi/nJ2b1PHNX4ErZmjqTXqUMMEIW5GKFbVKficXrX7FuSMoQ3se7daLXC4oZcw7nBeINGj6Aitr2W2tPjGkecgbhNxGO6KRMPex74IwF7IZ2zwisLNYOH7C03F/lU+8c2g6gcSMto3CYF7Xj4Nk2rzbn2hLdJ3d/Eh5OqnIyZ8L8/V9ini5kSp4bonILvJ67uifud7AbmwcdN6sD5MwIDAQAB"
        val publicKey = keyManager.loadPublicKey(keyId)
        (publicKey.toString.length) > 0 should be (true)

        val keyData = keyManager.getPublicKey(keyId)
        keyData should be(null)
    }
}
