package com.cubeone.scan.fraud

import android.graphics.Bitmap

object FraudEngine {

    fun analyze(bitmap: Bitmap, data: String): String {

        val screenshot = ScreenshotDetector.detect(bitmap)

        val tampered = ImageTamperDetector.detect(bitmap)

        return when {

            screenshot -> "FRAUD_RISK_SCREENSHOT"

            tampered -> "FRAUD_RISK_TAMPER"

            data.isEmpty() -> "NO_BARCODE"

            else -> "OK"
        }
    }
}