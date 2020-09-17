package org.ekstep.analytics.api.util.auth_verifier

import java.util
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.KeyFactory
import java.security.PublicKey
import java.security.spec.X509EncodedKeySpec
import java.util.Base64

import javax.inject.Singleton
import org.ekstep.analytics.api.util.APILogger
import org.ekstep.analytics.framework.conf.AppConf

import scala.collection.JavaConverters._

case class KeyData(keyId: String, publicKey: PublicKey)

@Singleton
class KeyManager {

    implicit val className = "org.ekstep.analytics.api.util.auth_verifier.KeyManager"
    val keyMap = new util.HashMap[String, KeyData]();

    def init() ={
        val basePath = AppConf.getConfig(JsonKey.ACCESS_TOKEN_PUBLICKEY_BASEPATH)
        try {
            val walk = Files.walk(Paths.get(basePath)).iterator().asScala
            val result = walk.filter(f => Files.isRegularFile(f))
            for (file <- result)
            {
                try {
                    val contentBuilder = StringBuilder.newBuilder
                    val path = Paths.get(file.toString);
                    for (x <- Files.lines(path, StandardCharsets.UTF_8).toArray) {
                        contentBuilder.append(x.toString)
                    }
                    val keyData = new KeyData(path.getFileName().toString(), loadPublicKey(contentBuilder.toString()));
                    keyMap.put(path.getFileName().toString(), keyData);
                }
                catch {
                    case ex: Exception =>
                        APILogger.log("KeyManager:init: exception in reading public keys: " + ex);
                }
            }
        } catch {
            case e: Exception =>
                APILogger.log("KeyManager:init: exception in loading publickeys: " + e);
        }
    }

    def getPublicKey(keyId: String): KeyData = {
        return keyMap.get(keyId);
    }

    def loadPublicKey(key: String): PublicKey = {
        var publicKey = new String(key.getBytes(), StandardCharsets.UTF_8);
        publicKey = publicKey.replaceAll("(-+BEGIN PUBLIC KEY-+)", "");
        publicKey = publicKey.replaceAll("(-+END PUBLIC KEY-+)", "");
        publicKey = publicKey.replaceAll("[\\r\\n]+", "");
        val keyBytes = Base64.getMimeDecoder.decode(publicKey.getBytes(StandardCharsets.UTF_8))

        val X509publicKey = new X509EncodedKeySpec(keyBytes);
        val kf = KeyFactory.getInstance("RSA");
        return kf.generatePublic(X509publicKey);
    }
}
