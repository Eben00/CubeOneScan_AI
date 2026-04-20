package com.cubeone.scan.plugins

import com.cubeone.scan.core.plugin.Plugin
import com.cubeone.scan.models.VehicleData // ADD THIS

object TradeInManager : Plugin {
    override val name: String = "Trade-In Manager"

    override fun initialize() {}

    override fun onVehicleScanned(vehicle: VehicleData) { // ADD OVERRIDE
        // Handle trade-in
    }

    override fun shutdown() {}
}