package com.cubeone.scan.models

data class StockUnit(
    val vin: String,
    val price: Double? = null,
    val location: String? = null
)