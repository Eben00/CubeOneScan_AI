package com.cubeone.scan.plugins

import com.cubeone.scan.core.plugin.Plugin
import com.cubeone.scan.models.VehicleData // ADD THIS

object StockTakeManager : Plugin {
    override val name: String = "Stock Take Manager"

    override fun initialize() {}

    override fun onVehicleScanned(vehicle: VehicleData) { // ADD OVERRIDE
        // Handle stock take
    }

    override fun shutdown() {}
}