package com.cubeone.scan.scanner

data class SALicenseData(
    val surname: String,
    val initials: String,
    val idNumber: String,
    val licenseNumber: String,
    val gender: String,
    val birthDate: String,
    val issueDate: String,
    val expiryDate: String,
    val vehicleCodes: List<String>,
    val rawBlocks: List<ByteArray>,
    val isValid: Boolean,
    val fraudFlags: List<String>,
    val photo: ByteArray? = null
)