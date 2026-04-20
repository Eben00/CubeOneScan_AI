package com.cubeone.scan.scanner

import android.content.Context
import android.util.Log
import com.cubeone.scan.models.VehicleData  // FIXED: Import VehicleData
import com.cubeone.scan.models.VehicleParser

class ScannerEngine(private val context: Context) {

    /**
     * Process Driver's License (ByteArray) - Decrypts and parses
     * Returns Map of fields (ID_NUMBER, SURNAME, NAMES, etc.)
     */
    fun processDriverLicense(data: ByteArray): Map<String, String> {
        Log.d("ScannerEngine", "Processing Driver License: ${data.size} bytes")

        // Validate data size
        if (data.size < 720) {
            Log.e("ScannerEngine", "Invalid data size: ${data.size}, expected 720")
            return mapOf("ERROR" to "Invalid data size: ${data.size}")
        }

        // FIXED: Decode returns ByteArray, then parse it
        val decryptedBytes = SADriverLicenseDecoder.decode(data)
        return DriverLicenseParser.parse(decryptedBytes)
    }

    /**
     * Process Vehicle License (String) - Parses % delimited text
     * Returns VehicleData object or null if parsing fails
     */
    fun processVehicleLicense(rawString: String): VehicleData? {
        Log.d("ScannerEngine", "Processing Vehicle License: ${rawString.length} chars")
        return VehicleParser.parse(rawString)
    }

    /**
     * Legacy support: Detects data type and processes accordingly
     * Returns Any that can be cast to Map<String, String> (Driver) or VehicleData (Vehicle)
     */
    fun processScannedData(data: Any): Any {
        return when (data) {
            is ByteArray -> processDriverLicense(data)
            is String -> processVehicleLicense(data) ?: mapOf("ERROR" to "Failed to parse vehicle data")
            else -> mapOf("ERROR" to "Unknown data type")
        }
    }
}