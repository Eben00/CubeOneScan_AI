package com.cubeone.scan.models

data class TradeIn(
    val vin: String,
    val mileage: Int? = null,
    val condition: String? = null
)