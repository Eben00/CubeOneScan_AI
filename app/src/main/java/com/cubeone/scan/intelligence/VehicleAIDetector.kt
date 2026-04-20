package com.cubeone.scan.intelligence

import android.graphics.Bitmap

data class VehicleDetection(
    val type: String,
    val confidence: Float
)

object VehicleAIDetector {

    fun detectVehicle(bitmap: Bitmap): VehicleDetection? {

        println("Running vehicle AI detection...")

        // Placeholder for AI model
        // Later we will load TensorFlow model here

        return VehicleDetection(
            type = "Sedan",
            confidence = 0.87f
        )
    }
}