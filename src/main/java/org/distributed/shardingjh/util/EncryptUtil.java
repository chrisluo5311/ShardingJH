package org.distributed.shardingjh.util;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

/**
 * Encryption helper clas
 * */
public class EncryptUtil {

    /**
     * Generates a HMAC-SHA256 hash for the given message using the provided secret key.
     *
     * @param message The input message to hash.
     * @param secret The secret key used for HMAC.
     * @return The resulting HMAC-SHA256 as a Base64-encoded string.
     * @throws RuntimeException if there is a cryptographic error.
     */
    public static String hmacSha256(String message, String secret) {
        try {
            // Create Mac instance for HmacSHA256 algorithm
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            // Create secret key specification from the secret string
            SecretKeySpec secret_key = new SecretKeySpec(secret.getBytes(), "HmacSHA256");
            // Initialize Mac instance with the secret key
            sha256_HMAC.init(secret_key);
            // Compute the HMAC on the input message
            byte[] hash = sha256_HMAC.doFinal(message.getBytes());
            // Return the Base64-encoded result
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            // Wrap and rethrow any exceptions as a runtime exception
            throw new RuntimeException("Error generating HMAC-SHA256", e);
        }
    }
}