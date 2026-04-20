package com.cubeone.scan.scanner

import android.Manifest
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.os.Bundle
import android.util.Log
import android.util.Size
import android.widget.Toast
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import androidx.camera.core.CameraSelector
import androidx.camera.core.ImageAnalysis
import androidx.camera.core.Preview
import androidx.camera.lifecycle.ProcessCameraProvider
import androidx.camera.view.PreviewView
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import com.cubeone.scan.ui.LicenseResultActivity
import com.cubeone.scan.ui.VehicleResultActivity
import com.cubeone.scan.models.VehicleData
import android.util.Base64
import java.io.ByteArrayOutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class ScannerActivity : AppCompatActivity() {

    private val tag = "ScannerActivity"
    private lateinit var previewView: PreviewView
    private lateinit var cameraExecutor: ExecutorService
    private var resultHandled: Boolean = false
    private var resultTextView: TextView? = null
    private var scanMode: String = MODE_ANY
    private var postScanAction: String = ACTION_NONE

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContentView(com.cubeone.scan.R.layout.activity_scanner)
        scanMode = intent.getStringExtra(EXTRA_SCAN_MODE)?.trim().orEmpty().ifBlank { MODE_ANY }
        postScanAction = intent.getStringExtra(EXTRA_POST_SCAN_ACTION)?.trim().orEmpty().ifBlank { ACTION_NONE }
        previewView = findViewById(com.cubeone.scan.R.id.previewView)
        resultTextView = findViewById(com.cubeone.scan.R.id.resultTextView)
        resultTextView?.text = when (scanMode) {
            MODE_DRIVER -> "Scanning driver license..."
            MODE_VEHICLE -> "Scanning vehicle license barcode..."
            else -> "Scanning..."
        }

        cameraExecutor = Executors.newSingleThreadExecutor()

        if (allPermissionsGranted()) {
            startCamera()
        } else {
            ActivityCompat.requestPermissions(
                this,
                REQUIRED_PERMISSIONS,
                REQUEST_CODE_PERMISSIONS
            )
        }
    }

    private fun startCamera() {
        val cameraProviderFuture = ProcessCameraProvider.getInstance(this)

        cameraProviderFuture.addListener({
            val cameraProvider = cameraProviderFuture.get()

            val preview = Preview.Builder()
                .build()
                .also {
                    it.setSurfaceProvider(previewView.surfaceProvider)
                }

            val imageAnalyzer = ImageAnalysis.Builder()
                .setTargetResolution(Size(1280, 720))
                .setBackpressureStrategy(ImageAnalysis.STRATEGY_KEEP_ONLY_LATEST)
                .build()
                .also {
                    it.setAnalyzer(
                        cameraExecutor,
                        BarcodeAnalyzer(
                            onLicenseDecoded = { result ->
                                onLicenseDecoded(result)
                            },
                            onVehicleScanned = { vehicle ->
                                onVehicleScanned(vehicle)
                            },
                            onQrScanned = { qr ->
                                onQrScanned(qr)
                            }
                        )
                    )
                }

            val cameraSelector = CameraSelector.DEFAULT_BACK_CAMERA

            try {
                cameraProvider.unbindAll()
                cameraProvider.bindToLifecycle(
                    this,
                    cameraSelector,
                    preview,
                    imageAnalyzer
                )
                Log.i(tag, "Camera started")
            } catch (e: Exception) {
                Log.e(tag, "Camera binding failed", e)
            }

        }, ContextCompat.getMainExecutor(this))
    }

    private fun onLicenseDecoded(data: Map<String, String>) {
        if (resultHandled) return
        if (scanMode == MODE_VEHICLE) {
            runOnUiThread {
                resultTextView?.text = "Vehicle mode: please scan vehicle barcode"
                Toast.makeText(this, "Please scan a vehicle license barcode", Toast.LENGTH_SHORT).show()
            }
            return
        }
        resultHandled = true

        runOnUiThread {
        Log.i(tag, "Driver license decoded")

            resultTextView?.text = "Driver license scanned"

            val intent = android.content.Intent(this, LicenseResultActivity::class.java).apply {
                putExtra("SUCCESS", true)
                putExtra("SURNAME", data["SURNAME"] ?: "")
                putExtra("NAMES", data["NAMES"] ?: "")
                putExtra("ID_NUMBER", data["ID_NUMBER"] ?: "")
                putExtra("LICENSE_NUMBER", data["LICENSE_NUMBER"] ?: "")
                putExtra("GENDER", data["GENDER"] ?: "")
                putExtra("DOB", data["DOB"] ?: "")
                putExtra("ISSUE_DATE", data["ISSUE_DATE"] ?: "")
                putExtra("EXPIRY_DATE", data["EXPIRY_DATE"] ?: "")
                // Not all licences yield these fields yet; keep empty for now.
                putExtra("VEHICLE_CODES", "")
                val embeddedPhoto = shrinkBase64Image(data["PHOTO"].orEmpty())
                val fallbackPhoto = capturePreviewPhotoBase64()
                val finalPhoto = if (embeddedPhoto.isNotBlank()) embeddedPhoto else fallbackPhoto
                putExtra("PHOTO", finalPhoto)
                putExtra("POST_SCAN_ACTION", postScanAction)
            }
            try {
                startActivity(intent)
                finish()
            } catch (e: Exception) {
                Log.e(tag, "Failed to open license result screen", e)
                resultHandled = false
                resultTextView?.text = "Scan complete, unable to open result"
            }
        }
    }

    private fun capturePreviewPhotoBase64(): String {
        return try {
            val bitmap: Bitmap = previewView.bitmap ?: return ""
            val out = ByteArrayOutputStream()
            val scaled = scaleBitmap(bitmap, 640)
            scaled.compress(Bitmap.CompressFormat.JPEG, 65, out)
            val bytes = out.toByteArray()
            if (bytes.isEmpty()) "" else Base64.encodeToString(bytes, Base64.NO_WRAP)
        } catch (_: Exception) {
            ""
        }
    }

    private fun shrinkBase64Image(base64: String): String {
        if (base64.isBlank()) return ""
        return try {
            val bytes = Base64.decode(base64, Base64.DEFAULT)
            if (bytes.isEmpty()) return ""
            val bitmap = BitmapFactory.decodeByteArray(bytes, 0, bytes.size) ?: return ""
            val scaled = scaleBitmap(bitmap, 640)
            val out = ByteArrayOutputStream()
            scaled.compress(Bitmap.CompressFormat.JPEG, 65, out)
            val compact = out.toByteArray()
            if (compact.isEmpty()) "" else Base64.encodeToString(compact, Base64.NO_WRAP)
        } catch (_: Exception) {
            ""
        }
    }

    private fun scaleBitmap(src: Bitmap, maxEdge: Int): Bitmap {
        val w = src.width
        val h = src.height
        if (w <= 0 || h <= 0) return src
        val largest = maxOf(w, h)
        if (largest <= maxEdge) return src
        val ratio = maxEdge.toFloat() / largest.toFloat()
        val nw = (w * ratio).toInt().coerceAtLeast(1)
        val nh = (h * ratio).toInt().coerceAtLeast(1)
        return Bitmap.createScaledBitmap(src, nw, nh, true)
    }

    private fun onVehicleScanned(vehicle: VehicleData) {
        if (resultHandled) return
        if (scanMode == MODE_DRIVER) {
            runOnUiThread {
                resultTextView?.text = "Driver mode: please scan driver license"
                Toast.makeText(this, "Please scan a driver license", Toast.LENGTH_SHORT).show()
            }
            return
        }
        resultHandled = true

        runOnUiThread {
        Log.i(tag, "Vehicle license decoded")

            resultTextView?.text = "Vehicle barcode scanned"

            val intent = android.content.Intent(this, VehicleResultActivity::class.java).apply {
                putExtra("SUCCESS", true)
                putExtra("REGISTRATION", vehicle.registration)
                putExtra("LICENCE_NUMBER", vehicle.licenceNumber)
                putExtra("MAKE", vehicle.make)
                putExtra("MODEL", vehicle.model)
                putExtra("VAL_MODEL", vehicle.valuationModel)
                putExtra("VAL_VARIANT", vehicle.valuationVariant)
                putExtra("COLOR", vehicle.color)
                putExtra("VIN", vehicle.vin)
                putExtra("ENGINE_NUMBER", vehicle.engineNumber)
                putExtra("EXPIRY", vehicle.expiry)
                putExtra("FIRST_REG_DATE", vehicle.firstRegistrationDate)
                putExtra("FIRST_REG_YEAR", vehicle.firstRegistrationYear)
                putExtra("RAW_PAYLOAD", vehicle.rawPayload)
                putExtra("POST_SCAN_ACTION", postScanAction)
            }
            startActivity(intent)
            finish()
        }
    }

    private fun onQrScanned(qrValue: String) {
        if (resultHandled) return
        resultHandled = true

        runOnUiThread {
            Log.i(tag, "QR code decoded")
            resultTextView?.text = "QR code scanned"
            Toast.makeText(this, "QR: $qrValue", Toast.LENGTH_LONG).show()
            // Keep scanner screen open for now so user can continue scanning after QR confirmation.
            resultHandled = false
        }
    }

    private fun allPermissionsGranted(): Boolean {
        return REQUIRED_PERMISSIONS.all {
            ContextCompat.checkSelfPermission(baseContext, it) == PackageManager.PERMISSION_GRANTED
        }
    }

    override fun onRequestPermissionsResult(
        requestCode: Int,
        permissions: Array<String>,
        grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == REQUEST_CODE_PERMISSIONS) {
            if (allPermissionsGranted()) {
                startCamera()
            } else {
                Log.e(tag, "Camera permission denied")
                finish()
            }
        }
    }

    override fun onDestroy() {
        super.onDestroy()
        cameraExecutor.shutdown()
    }

    companion object {
        private const val REQUEST_CODE_PERMISSIONS = 10
        private val REQUIRED_PERMISSIONS = arrayOf(Manifest.permission.CAMERA)
        const val EXTRA_SCAN_MODE = "scan_mode"
        const val EXTRA_POST_SCAN_ACTION = "post_scan_action"
        const val MODE_ANY = "any"
        const val MODE_DRIVER = "driver_license"
        const val MODE_VEHICLE = "vehicle_license"
        const val ACTION_NONE = "none"
        const val ACTION_STOCK_TAKE = "stock_take"
        const val ACTION_TRADE_IN = "trade_in"
        const val ACTION_SHARE_LEAD = "share_lead"
        const val ACTION_SHARE_STOCK = "share_stock"
    }
}