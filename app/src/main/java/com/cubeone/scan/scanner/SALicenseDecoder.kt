package com.cubeone.scan.scanner

import android.util.Base64
import android.util.Log
import java.math.BigInteger

object SADriverLicenseDecoder {

    private const val TAG = "SADL"

    // Key components initialized via lazy to avoid destructuring issues
    private val keyComponents: Pair<BigInteger, BigInteger> by lazy {
        parsePEMKey(
            "MIGWAoGBAMqfGO9sPz+kxaRh/qVKsZQGul7NdG1gonSS3KPXTjtcHTFfexA4MkGA" +
                    "mwKeu9XeTRFgMMxX99WmyaFvNzuxSlCFI/foCkx0TZCFZjpKFHLXryxWrkG1Bl9+" +
                    "+gKTvTJ4rWk1RvnxYhm3n/Rxo2NoJM/822Oo7YBZ5rmk8NuJU4HLAhAYcJLaZFTO" +
                    "sYU+aRX4RmoF"
        )
    }

    private val rsaModulus: BigInteger
        get() = keyComponents.first

    private val rsaExponent: BigInteger
        get() = keyComponents.second

    /**
     * Parse PKCS#1 RSA Public Key ASN.1 structure
     */
    private fun parsePEMKey(base64Key: String): Pair<BigInteger, BigInteger> {
        val keyBytes = Base64.decode(base64Key, Base64.DEFAULT)

        var offset = 0

        // Check SEQUENCE tag (0x30)
        if (keyBytes[offset++] != 0x30.toByte()) {
            throw IllegalArgumentException("Expected SEQUENCE tag")
        }

        // Read sequence length
        var length = keyBytes[offset++].toInt() and 0xFF
        if (length and 0x80 != 0) {
            val numBytes = length and 0x7F
            length = 0
            repeat(numBytes) {
                length = (length shl 8) or (keyBytes[offset++].toInt() and 0xFF)
            }
        }

        // Parse modulus INTEGER
        if (keyBytes[offset++] != 0x02.toByte()) {
            throw IllegalArgumentException("Expected INTEGER for modulus")
        }

        // Read modulus length
        var modLength = keyBytes[offset].toInt() and 0xFF
        var lengthBytes = 1
        if (modLength and 0x80 != 0) {
            val numLenBytes = modLength and 0x7F
            modLength = 0
            for (i in 1..numLenBytes) {
                modLength = (modLength shl 8) or (keyBytes[offset + i].toInt() and 0xFF)
            }
            lengthBytes = 1 + numLenBytes
        }
        offset += lengthBytes
        val modulusBytes = keyBytes.copyOfRange(offset, offset + modLength)
        offset += modLength

        // Parse exponent INTEGER
        if (keyBytes[offset++] != 0x02.toByte()) {
            throw IllegalArgumentException("Expected INTEGER for exponent")
        }

        // Read exponent length
        var expLength = keyBytes[offset].toInt() and 0xFF
        lengthBytes = 1
        if (expLength and 0x80 != 0) {
            val numLenBytes = expLength and 0x7F
            expLength = 0
            for (i in 1..numLenBytes) {
                expLength = (expLength shl 8) or (keyBytes[offset + i].toInt() and 0xFF)
            }
            lengthBytes = 1 + numLenBytes
        }
        offset += lengthBytes
        val exponentBytes = keyBytes.copyOfRange(offset, offset + expLength)

        val modulus = BigInteger(1, modulusBytes)
        val exponent = BigInteger(1, exponentBytes)

        Log.d(TAG, "Key parsed: ${modulus.bitLength()} bit modulus, exponent=$exponent")
        return Pair(modulus, exponent)
    }

    fun decode(rawBytes: ByteArray): ByteArray {
        Log.d(TAG, "RAW SIZE: ${rawBytes.size}")

        if (rawBytes.size < 720) {
            throw IllegalArgumentException("Expected 720 bytes, got ${rawBytes.size}")
        }

        val result = mutableListOf<Byte>()

        // Decrypt 5 blocks of 128 bytes (starting at offset 6 after header)
        for (i in 0..4) {
            val start = 6 + (i * 128)
            val encryptedBlock = rawBytes.copyOfRange(start, start + 128)

            // BigInteger(1, ...) ensures unsigned/positive interpretation
            val input = BigInteger(1, encryptedBlock)
            val output = input.modPow(rsaExponent, rsaModulus)

            var decrypted = output.toByteArray()

            // Normalize to exactly 128 bytes
            decrypted = when {
                decrypted.size == 129 && decrypted[0] == 0.toByte() -> {
                    decrypted.copyOfRange(1, 129) // Remove sign byte
                }
                decrypted.size < 128 -> {
                    ByteArray(128 - decrypted.size) { 0 } + decrypted // Pad
                }
                decrypted.size > 128 -> {
                    decrypted.copyOfRange(decrypted.size - 128, decrypted.size) // Take last 128
                }
                else -> decrypted
            }

            result.addAll(decrypted.toList())
        }

        // Add final unencrypted block (74 bytes at offset 646)
        result.addAll(rawBytes.copyOfRange(646, 720).toList())

        val finalResult = result.toByteArray()
        Log.d(TAG, "DECRYPTION COMPLETE: ${finalResult.size} bytes")
        return finalResult
    }
}