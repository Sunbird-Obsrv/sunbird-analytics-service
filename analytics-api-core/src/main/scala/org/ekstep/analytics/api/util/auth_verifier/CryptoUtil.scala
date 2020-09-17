package org.ekstep.analytics.api.util.auth_verifier

import java.nio.charset.Charset
import java.security._

import javax.inject.Singleton;

@Singleton
class CryptoUtil {

    val US_ASCII = Charset.forName("US-ASCII");

    def verifyRSASign(payLoad: String, signature: Array[Byte], key: PublicKey, algorithm: String): Boolean = {
        try {
            val sign = Signature.getInstance(algorithm);
            sign.initVerify(key);
            sign.update(payLoad.getBytes(US_ASCII));
            return sign.verify(signature);
        }
        catch {
            case ex @ (_ : NoSuchAlgorithmException | _ : NoSuchAlgorithmException | _ :SignatureException ) =>
                return false
        }
    }
}