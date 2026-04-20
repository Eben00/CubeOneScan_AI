import android.util.Log
import java.math.BigInteger
import java.security.KeyFactory
import java.security.spec.RSAPublicKeySpec
import javax.crypto.Cipher

data class VehicleLicenseData(
    val licenseNumber: String,
    val registrationNumber: String,
    val vehicleMake: String,
    val vehicleModel: String,
    val vehicleColor: String,
    val vinNumber: String,
    val engineNumber: String,
    val expiryDate: String,
    val issueDate: String,
    val ownerName: String? = null,
    val ownerId: String? = null
)

object VehicleLicenseDecoder {
    private const val TAG = "VehicleLicenseDecoder"

    // SA Vehicle License RSA Public Key (different from Driver's License!)
    private val MODULUS = BigInteger("YOUR_VEHICLE_LICENSE_MODULUS_HERE", 16)
    private val EXPONENT = BigInteger("10001", 16) // 65537

    fun decode(rawBytes: ByteArray): VehicleLicenseData? {
        Log.d(TAG, "Decoding ${rawBytes.size} bytes")

        return try {
            // Step 1: Decrypt (Vehicle licenses are also encrypted)
            val decrypted = decryptIfNeeded(rawBytes)
            Log.d(TAG, "Decrypted size: ${decrypted.size}")

            // Step 2: Parse TLV structure
            parseVehicleLicense(decrypted)

        } catch (e: Exception) {
            Log.e(TAG, "Decode error", e)
            null
        }
    }

    private fun decryptIfNeeded(data: ByteArray): ByteArray {
        // If data starts with 0x30, it's likely already decrypted DER/BER
        if (data[0] == 0x30.toByte() && data.size > 200) {
            Log.d(TAG, "Data appears unencrypted (starts with 0x30)")
            return data
        }

        // Otherwise decrypt using RSA
        return try {
            val keySpec = RSAPublicKeySpec(MODULUS, EXPONENT)
            val keyFactory = KeyFactory.getInstance("RSA")
            val publicKey = keyFactory.generatePublic(keySpec)

            val cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding")
            cipher.init(Cipher.DECRYPT_MODE, publicKey)

            // Vehicle licenses might be in multiple blocks
            val blockSize = 128 // 1024-bit key = 128 bytes
            val decrypted = mutableListOf<Byte>()

            for (i in data.indices step blockSize) {
                val end = minOf(i + blockSize, data.size)
                val block = data.copyOfRange(i, end)
                if (block.size == blockSize) {
                    decrypted.addAll(cipher.doFinal(block).toList())
                }
            }

            decrypted.toByteArray()
        } catch (e: Exception) {
            Log.e(TAG, "Decryption failed", e)
            data // Return original if decrypt fails
        }
    }

    private fun parseVehicleLicense(data: ByteArray): VehicleLicenseData? {
        val fields = mutableMapOf<Int, String>()
        var pos = 0

        while (pos < data.size - 2) {
            // Skip sequence/set tags
            if (data[pos] == 0x30.toByte() || data[pos] == 0x31.toByte()) {
                pos++
                continue
            }

            // Read tag
            val tag = data[pos].toInt() and 0xFF
            pos++

            if (pos >= data.size) break

            // Read length
            var length = data[pos].toInt() and 0xFF
            pos++

            // Handle multi-byte length
            if (length > 0x80) {
                val numBytes = length - 0x80
                length = 0
                repeat(numBytes) {
                    if (pos < data.size) {
                        length = (length shl 8) or (data[pos].toInt() and 0xFF)
                        pos++
                    }
                }
            }

            if (length < 0 || length > 500 || pos + length > data.size) {
                pos++
                continue
            }

            // Read value
            val valueBytes = data.copyOfRange(pos, pos + length)
            val value = String(valueBytes).trim()
            fields[tag] = value
            pos += length

            Log.v(TAG, "Tag 0x${"%02X".format(tag)} (${tag}): $value")
        }

        // SA Vehicle License Tag Mapping (these vary by issuing authority)
        // Common tags based on analysis:
        // 0x01 = License Number
        // 0x02 = Registration Number
        // 0x03 = Vehicle Make
        // 0x04 = Vehicle Model
        // 0x05 = VIN
        // 0x06 = Engine Number
        // 0x07 = Expiry Date (YYYYMMDD)
        // 0x08 = Issue Date
        // 0x09 = Vehicle Color
        // 0x0A = Owner Name

        return VehicleLicenseData(
            licenseNumber = fields[0x01] ?: fields[0x81] ?: "Unknown",
            registrationNumber = fields[0x02] ?: fields[0x82] ?: "Unknown",
            vehicleMake = fields[0x03] ?: fields[0x83] ?: "",
            vehicleModel = fields[0x04] ?: fields[0x84] ?: "",
            vehicleColor = fields[0x09] ?: fields[0x89] ?: "",
            vinNumber = fields[0x05] ?: fields[0x85] ?: "",
            engineNumber = fields[0x06] ?: fields[0x86] ?: "",
            expiryDate = fields[0x07] ?: fields[0x87] ?: "",
            issueDate = fields[0x08] ?: fields[0x88] ?: "",
            ownerName = fields[0x0A] ?: fields[0x8A]
        )
    }
}