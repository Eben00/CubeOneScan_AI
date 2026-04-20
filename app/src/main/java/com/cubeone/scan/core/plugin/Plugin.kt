package com.cubeone.scan.core.plugin

import com.cubeone.scan.models.VehicleData
interface Plugin {
    val name: String
    fun initialize()
    fun onLicenseScanned(data: Map<String, String>) {}
    fun onVehicleScanned(vehicle: VehicleData) {}
    fun shutdown()
}