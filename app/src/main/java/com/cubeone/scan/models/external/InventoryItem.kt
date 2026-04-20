package com.cubeone.scan.models.external

data class InventoryItem(
    val vin: String,
    val registration: String,
    val make: String,
    val model: String,
    val year: Int? = null,
    val color: String? = null,
    val engineNumber: String? = null,
    val source: String = "SCAN",
    val status: String = "PENDING",
    val scanDate: Long = System.currentTimeMillis()
)