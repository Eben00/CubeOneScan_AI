package com.cubeone.scan.models

data class DriverLicense(
    val firstName: String,
    val surname: String,
    val idNumber: String,
    val licenseNumber: String,
    val dateOfBirth: String,
    val expiryDate: String
)