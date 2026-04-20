package com.cubeone.scan.models

data class VehicleData(
    val registration: String = "",
    val licenceNumber: String = "",
    val make: String = "",
    /** Raw line from licence disc (may include make + model + derivative). */
    val model: String = "",
    /**
     * Valuation-style base model token (e.g. JAZZ) when we can infer it from [model];
     * blank if unknown — callers fall back to [model].
     */
    val valuationModel: String = "",
    /**
     * Valuation-style variant/derivative (e.g. 1.5I EX AT) when inferable; else blank.
     */
    val valuationVariant: String = "",
    val color: String = "",
    val vin: String = "",
    val engineNumber: String = "",
    val expiry: String = "",
    val firstRegistrationDate: String = "",
    val firstRegistrationYear: String = "",
    val rawPayload: String = ""
)