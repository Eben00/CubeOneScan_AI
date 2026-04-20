package com.cubeone.scan.models.external

data class CrmLead(
    val source: String = "CubeOneScan",
    val scanType: String,
    val timestamp: Long = System.currentTimeMillis(),

    val idNumber: String? = null,
    val surname: String? = null,
    val firstName: String? = null,
    val licenseNumber: String? = null,
    val dateOfBirth: String? = null,
    val gender: String? = null,
    val expiryDate: String? = null,

    val vin: String? = null,
    val registration: String? = null,
    val make: String? = null,
    val model: String? = null,
    val engineNumber: String? = null,
    val vehicleExpiry: String? = null,

    val deviceId: String? = null,
    val location: String? = null
) {
    fun toJson(): String {
        return """
        {
            "source": "$source",
            "scan_type": "$scanType",
            "timestamp": $timestamp,
            "customer": {
                "id_number": "${idNumber ?: ""}",
                "surname": "${surname ?: ""}",
                "first_name": "${firstName ?: ""}",
                "license_number": "${licenseNumber ?: ""}",
                "date_of_birth": "${dateOfBirth ?: ""}",
                "gender": "${gender ?: ""}",
                "license_expiry": "${expiryDate ?: ""}"
            },
            "vehicle": {
                "vin": "${vin ?: ""}",
                "registration": "${registration ?: ""}",
                "make": "${make ?: ""}",
                "model": "${model ?: ""}",
                "engine_number": "${engineNumber ?: ""}",
                "license_expiry": "${vehicleExpiry ?: ""}"
            },
            "metadata": {
                "device_id": "${deviceId ?: ""}",
                "location": "${location ?: ""}"
            }
        }
        """.trimIndent()
    }
}