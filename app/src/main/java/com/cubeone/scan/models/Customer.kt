package com.cubeone.scan.models

data class Customer(
    val firstName: String,
    val surname: String,
    val idNumber: String,
    val driverLicenseNumber: String? = null,
    val birthDate: String? = null,
    val gender: String? = null,
    val phone: String? = null,
    val email: String? = null,
    val address: String? = null
)