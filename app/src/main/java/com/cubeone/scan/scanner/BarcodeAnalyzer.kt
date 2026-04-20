package com.cubeone.scan.scanner

import android.annotation.SuppressLint
import android.media.AudioManager
import android.media.ToneGenerator
import android.util.Log
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.ImageProxy
import com.google.mlkit.vision.common.InputImage
import com.google.mlkit.vision.barcode.common.Barcode
import com.google.mlkit.vision.barcode.BarcodeScannerOptions
import com.google.mlkit.vision.barcode.BarcodeScanning
import com.cubeone.scan.models.VehicleData
import com.cubeone.scan.models.VehicleParser

class BarcodeAnalyzer(
    private val onLicenseDecoded: (Map<String, String>) -> Unit,
    private val onVehicleScanned: (VehicleData) -> Unit,
    private val onQrScanned: (String) -> Unit
) : ImageAnalysis.Analyzer {

    private val tone = ToneGenerator(AudioManager.STREAM_MUSIC, 100)
    private val scanner = BarcodeScanning.getClient(
        BarcodeScannerOptions.Builder()
            .setBarcodeFormats(
                Barcode.FORMAT_PDF417,
                Barcode.FORMAT_QR_CODE
            )
            .build()
    )
    private var processing = false

    @SuppressLint("UnsafeOptInUsageError")
    override fun analyze(imageProxy: ImageProxy) {
        if (processing) {
            imageProxy.close()
            return
        }

        val mediaImage = imageProxy.image ?: run {
            imageProxy.close()
            return
        }

        val image = InputImage.fromMediaImage(mediaImage, imageProxy.imageInfo.rotationDegrees)
        processing = true

        scanner.process(image)
            .addOnSuccessListener { barcodes ->
                for (barcode in barcodes) {
                    processBarcode(barcode)
                }
            }
            .addOnCompleteListener {
                processing = false
                imageProxy.close()
            }
    }

    private fun processBarcode(barcode: Barcode) {
        val rawBytes = barcode.rawBytes
        val rawValue = barcode.rawValue ?: ""

        when {
            // Driver's License: Binary >600 bytes
            rawBytes != null && rawBytes.size > 600 -> {
                try {
                    val decrypted = SADriverLicenseDecoder.decode(rawBytes)
                    val result = DriverLicenseParser.parse(decrypted)
                    Log.i("SADL", "DRIVER: ${result["ID_NUMBER"]}")
                    tone.startTone(ToneGenerator.TONE_PROP_BEEP, 150)
                    onLicenseDecoded(result)
                } catch (e: Exception) {
                    Log.e("SADL", "Failed: ${e.message}")
                }
            }

            // Vehicle License: Text with % delimiters
            rawValue.contains("%") && rawValue.length > 50 -> {
                VehicleParser.parse(rawValue)?.let { vehicle ->
                    Log.i("Vehicle", "VEHICLE: ${vehicle.registration}")
                    tone.startTone(ToneGenerator.TONE_PROP_BEEP, 150)
                    onVehicleScanned(vehicle)
                }
            }

            // Generic QR payload
            barcode.format == Barcode.FORMAT_QR_CODE && rawValue.isNotBlank() -> {
                Log.i("QR", "QR scanned")
                tone.startTone(ToneGenerator.TONE_PROP_BEEP, 150)
                onQrScanned(rawValue)
            }
        }
    }
}