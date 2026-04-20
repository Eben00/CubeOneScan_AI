package com.cubeone.scan

import android.util.Log
import com.cubeone.scan.scanner.ScanResult

object PluginManager {

    fun registerDefaults() {
        Log.d("PluginManager", "Plugins registered")
    }

    fun dispatch(result: ScanResult) {
        when (result) {
            is ScanResult.DriverLicense -> {
                Log.i("PluginManager", "Driver License: ${result.surname} / ${result.idNumber}")
                // TODO: Connect to CRM/Lead creation
            }
            is ScanResult.VehicleDisk -> {
                Log.i("PluginManager", "Vehicle Disk: ${result.registration}")
                // TODO: Connect to DMS/Stock
            }
        }
    }
}