package com.cubeone.scan.models.external

data class TradeInValuationRequest(
    val vin: String,
    val mileage: Int? = null,
    val condition: String = "good",
    val zipCode: String? = null
)

data class TradeInValuationResponse(
    val success: Boolean,
    val tradeInValue: Double?,
    val retailValue: Double?,
    val wholesaleValue: Double?,
    val message: String?
)