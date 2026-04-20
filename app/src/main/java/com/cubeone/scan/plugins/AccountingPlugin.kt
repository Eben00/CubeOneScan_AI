package com.cubeone.scan.plugins

import android.util.Log
import com.cubeone.scan.core.plugin.Plugin
import com.cubeone.scan.models.VehicleData // ADD THIS

object AccountingPlugin : Plugin {
    override val name: String = "Accounting Plugin"

    override fun initialize() {
        Log.d("Accounting", "Init")
    }

    override fun onLicenseScanned(data: Map<String, String>) {
        Log.d("Accounting", "License: ${data["SURNAME"]}")
    }

    override fun onVehicleScanned(vehicle: VehicleData) { // ADD OVERRIDE
        Log.d("Accounting", "Vehicle: ${vehicle.registration}")
    }

    override fun shutdown() {}
}