package com.cubeone.scan.scanner

sealed class ScanResult {
    data class DriverLicense(
        val idNumber: String,
        val surname: String,
        val names: String,
        val expiryDate: String,
        val dob: String,
        val gender: String
    ) : ScanResult()

    data class VehicleDisk(
        val vin: String,
        val engineNumber: String,
        val registration: String,
        val make: String,
        val model: String,
        val expiryDate: String
    ) : ScanResult()
}