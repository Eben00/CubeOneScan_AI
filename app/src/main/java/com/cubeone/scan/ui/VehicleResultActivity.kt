package com.cubeone.scan.ui

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.widget.NestedScrollView
import androidx.core.content.FileProvider
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.services.CommandApiService
import com.cubeone.scan.utils.WorkflowState
import com.google.android.material.button.MaterialButton
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class VehicleResultActivity : AppCompatActivity() {
    private data class DamageEntry(
        val marker: DamageMarker,
        var description: String,
        var reconCost: Long,
        var severity: String
    )

    private val tradeInPhotoUris = mutableMapOf<String, Uri>()
    private var pendingCaptureView: String? = null
    private val damageEntries = mutableListOf<DamageEntry>()
    private var damageWireframeView: DamageWireframeView? = null
    private var tvDamagesSummary: TextView? = null
    private val takePictureLauncher = registerForActivityResult(ActivityResultContracts.TakePicture()) { success ->
        val key = pendingCaptureView
        if (!success || key == null) {
            Toast.makeText(this, "Photo capture cancelled", Toast.LENGTH_SHORT).show()
            return@registerForActivityResult
        }
        val uri = tradeInPhotoUris[key]
        updatePhotoStatus(key, uri)
    }

    private fun normalizeBusinessRole(rawRole: String): String {
        val role = rawRole.trim().lowercase()
        return when (role) {
            "dealer_principal", "sales_manager", "sales_person" -> role
            "superadmin", "owner", "admin" -> "dealer_principal"
            "agent" -> "sales_person"
            else -> "sales_person"
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_vehicle_result)

        val registration = intent.getStringExtra("REGISTRATION") ?: ""
        val licenceNumber = intent.getStringExtra("LICENCE_NUMBER") ?: ""
        val make = intent.getStringExtra("MAKE") ?: ""
        val model = intent.getStringExtra("MODEL") ?: ""
        val valuationModelHint = intent.getStringExtra("VAL_MODEL")
            ?: intent.getStringExtra("TT_MODEL")
            ?: ""
        val valuationVariantHint = intent.getStringExtra("VAL_VARIANT")
            ?: intent.getStringExtra("TT_VARIANT")
            ?: ""
        val modelForValuation = valuationModelHint.ifBlank { model }
        val color = intent.getStringExtra("COLOR") ?: ""
        val vin = intent.getStringExtra("VIN") ?: ""
        val engineNumber = intent.getStringExtra("ENGINE_NUMBER") ?: ""
        val expiry = intent.getStringExtra("EXPIRY") ?: ""
        val firstRegDate = intent.getStringExtra("FIRST_REG_DATE") ?: ""
        val firstRegYear = intent.getStringExtra("FIRST_REG_YEAR") ?: ""
        val rawPayload = intent.getStringExtra("RAW_PAYLOAD") ?: ""
        val postScanAction = intent.getStringExtra("POST_SCAN_ACTION") ?: "none"
        val role = normalizeBusinessRole(AuthStore.getRole(this).orEmpty())

        findViewById<TextView>(R.id.tvRegistration)?.text = "Registration: $registration"
        findViewById<TextView>(R.id.tvLicenceNumber)?.text = "Licence No: $licenceNumber"
        findViewById<TextView>(R.id.tvMake)?.text = "Make: $make"
        findViewById<TextView>(R.id.tvModel)?.text = "Model: $model"
        findViewById<TextView>(R.id.tvColor)?.text = "Color: $color"
        findViewById<TextView>(R.id.tvVin)?.text = "VIN: $vin"
        findViewById<TextView>(R.id.tvEngineNumber)?.text = "Engine: $engineNumber"
        findViewById<TextView>(R.id.tvExpiry)?.text = "Expiry: $expiry"
        findViewById<TextView>(R.id.tvFirstReg)?.text =
            "First registration: ${if (firstRegDate.isNotBlank()) firstRegDate else "—"}" +
                (if (firstRegYear.isNotBlank()) " (year $firstRegYear)" else "")
        val hasStructuredVehicleData = listOf(
            registration,
            licenceNumber,
            make,
            model,
            vin,
            engineNumber,
            expiry,
            firstRegYear
        ).any { it.isNotBlank() }
        val tvRawPayload = findViewById<TextView>(R.id.tvRawPayload)
        if (hasStructuredVehicleData) {
            tvRawPayload?.visibility = View.GONE
        } else {
            tvRawPayload?.visibility = View.VISIBLE
            tvRawPayload?.text =
                if (rawPayload.isBlank()) "Raw payload: —"
                else "Raw payload: ${rawPayload.take(180)}"
        }

        val cardValuation = findViewById<View>(R.id.cardValuation)
        val scrollRoot = findViewById<NestedScrollView>(R.id.svVehicleRoot)
        val tvTradeValue = findViewById<TextView>(R.id.tvTradeValue)
        val tvRetailValue = findViewById<TextView>(R.id.tvRetailValue)
        val tvMarketValue = findViewById<TextView>(R.id.tvMarketValue)
        val tvValuationMeta = findViewById<TextView>(R.id.tvValuationMeta)
        val etYear = findViewById<EditText>(R.id.etYear)
        val etVariant = findViewById<EditText>(R.id.etVariant)

        if (firstRegYear.isNotBlank()) {
            etYear.setText(firstRegYear)
        }
        when {
            valuationVariantHint.isNotBlank() -> etVariant.setText(valuationVariantHint)
            model.isNotBlank() -> etVariant.setText(model)
            make.isNotBlank() -> etVariant.setText(make)
        }

        cardValuation?.visibility = View.GONE

        findViewById<MaterialButton>(R.id.btnDone)?.setOnClickListener {
            finish()
        }

        // ===============================
        // Market Valuation
        // ===============================
        findViewById<MaterialButton>(R.id.btnGetValuation)?.setOnClickListener {
            val yearStr = etYear.text?.toString()?.trim().orEmpty()
            val variant = etVariant.text?.toString()?.trim().orEmpty()
            val mileageStr = findViewById<EditText>(R.id.etMileage).text?.toString()?.trim().orEmpty()

            if (make.isBlank() || modelForValuation.isBlank()) {
                Toast.makeText(this, "Make and model required for valuation", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            if (yearStr.isBlank() || variant.isBlank() || mileageStr.isBlank()) {
                Toast.makeText(this, "Year, variant and mileage are required", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val year = yearStr.toIntOrNull()
            val mileage = mileageStr.toIntOrNull()
            if (year == null || mileage == null) {
                Toast.makeText(this, "Year and mileage must be numbers", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val payload = JSONObject().apply {
                put("make", make)
                put("model", modelForValuation)
                put("variant", variant)
                put("rawModel", model)
                put("rawVariant", valuationVariantHint.ifBlank { model })
                put("year", year)
                put("mileage", mileage)
            }

            CommandApiService.createCommand(
                context = this,
                commandType = "MARKET_VALUE_REPORT",
                payload = payload,
                onSuccess = { resp ->
                    runOnUiThread {
                        val body = resp.result
                        if (resp.status == "done" && body != null) {
                            applyValuationResult(body, tvTradeValue, tvRetailValue, tvMarketValue, tvValuationMeta)
                            cardValuation?.visibility = View.VISIBLE
                            Toast.makeText(this, "Valuation loaded", Toast.LENGTH_SHORT).show()
                        } else {
                            Toast.makeText(this, "Unexpected response from connector", Toast.LENGTH_LONG).show()
                        }
                    }
                },
                onError = { err ->
                    runOnUiThread {
                        AlertDialog.Builder(this)
                            .setTitle("Valuation failed")
                            .setMessage(err)
                            .setPositiveButton("OK", null)
                            .show()
                    }
                }
            )
        }

        // ===============================
        // DMS WORKFLOW BUTTONS (v1)
        // ===============================
        findViewById<MaterialButton>(R.id.btnCreateStockUnit)?.setOnClickListener {
            val year = findViewById<EditText>(R.id.etYear).text?.toString()?.trim().orEmpty()
            val variant = findViewById<EditText>(R.id.etVariant).text?.toString()?.trim().orEmpty()
            val mileage = findViewById<EditText>(R.id.etMileage).text?.toString()?.trim().orEmpty()
            val price = findViewById<EditText>(R.id.etPrice).text?.toString()?.trim().orEmpty()
            val category = findViewById<EditText>(R.id.etCategory).text?.toString()?.trim().orEmpty()
            val notes = findViewById<EditText>(R.id.etStockNotes).text?.toString()?.trim().orEmpty()
            val vehicle = JSONObject().apply {
                put("registration", registration)
                put("licenceNumber", licenceNumber)
                put("make", make)
                put("model", model)
                put("variant", variant)
                put("year", year)
                put("mileage", mileage)
                put("price", price)
                put("color", color)
                put("vin", vin)
                put("engineNumber", engineNumber)
                put("expiry", expiry)
                put("firstRegistrationDate", firstRegDate)
                put("firstRegistrationYear", firstRegYear)
                put("category", category)
                put("notes", notes)
            }

            val payload = JSONObject().apply {
                put("vehicle", vehicle)
            }

            CommandApiService.createCommand(
                context = this,
                commandType = "CREATE_STOCK_UNIT",
                payload = payload,
                onSuccess = { resp ->
                    WorkflowState.setStockCorrelationId(this, resp.correlationId)
                    runOnUiThread {
                        Toast.makeText(this, "Stock queued: ${resp.correlationId}", Toast.LENGTH_LONG).show()
                    }
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Create stock failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        val btnTradeIn = findViewById<MaterialButton>(R.id.btnTradeIn)
        val btnStockTake = findViewById<MaterialButton>(R.id.btnStockTake)
        if (role == "sales_person") {
            btnTradeIn?.text = "Submit trade-in for manager approval"
            btnStockTake?.text = "Run stock take"
        }

        bindTradeInCaptureUi()

        if (postScanAction == "stock_take") {
            Toast.makeText(this, "Vehicle captured. Review details, then tap Stock Take.", Toast.LENGTH_LONG).show()
            focusAction(scrollRoot, findViewById(R.id.btnStockTake))
        } else if (postScanAction == "trade_in") {
            Toast.makeText(this, "Vehicle captured. Add damages/photos, then tap Trade-In.", Toast.LENGTH_LONG).show()
            focusAction(scrollRoot, findViewById(R.id.btnTradeIn))
        }

        btnTradeIn?.setOnClickListener {
            val stockCorrelationId = WorkflowState.getStockCorrelationId(this)
            if (stockCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create stock unit first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val frontPhoto = tradeInPhotoUris["front"]?.toString().orEmpty()
            val leftPhoto = tradeInPhotoUris["left"]?.toString().orEmpty()
            val rightPhoto = tradeInPhotoUris["right"]?.toString().orEmpty()
            val backPhoto = tradeInPhotoUris["back"]?.toString().orEmpty()
            if (listOf(frontPhoto, leftPhoto, rightPhoto, backPhoto).any { it.isBlank() }) {
                Toast.makeText(this, "Trade-in photos required: front, left, right, back", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            if (damageEntries.isEmpty()) {
                Toast.makeText(this, "Add at least one damage item with recon cost", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val wireframeMarkers = JSONArray().apply {
                damageEntries.forEachIndexed { index, entry ->
                    put(
                        JSONObject().apply {
                            put("index", index + 1)
                            put("zone", entry.marker.zone)
                            put("x", entry.marker.xRatio)
                            put("y", entry.marker.yRatio)
                        }
                    )
                }
            }
            val damages = JSONArray().apply {
                damageEntries.forEachIndexed { index, entry ->
                    put(
                        JSONObject().apply {
                            put("index", index + 1)
                            put("zone", entry.marker.zone)
                            put("description", entry.description)
                            put("reconCost", entry.reconCost)
                            put("severity", entry.severity)
                        }
                    )
                }
            }

            val payload = JSONObject().apply {
                put("stockCorrelationId", stockCorrelationId)
                put("vin", vin)
                put("mileage", JSONObject.NULL)
                put("condition", "")
                put(
                    "photos",
                    JSONObject().apply {
                        put("front", frontPhoto)
                        put("left", leftPhoto)
                        put("right", rightPhoto)
                        put("back", backPhoto)
                    }
                )
                put("damageWireframe", JSONObject().apply { put("markers", wireframeMarkers) })
                put("damages", damages)
                put("currency", "ZAR")
            }

            CommandApiService.createCommand(
                context = this,
                commandType = "TRADE_IN",
                payload = payload,
                onSuccess = { resp ->
                    runOnUiThread {
                        val msg = if (role == "sales_person") {
                            "Trade-in submitted for manager approval: ${resp.correlationId}"
                        } else {
                            "Trade-in queued: ${resp.correlationId}"
                        }
                        Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                    }
                    if (role == "sales_person") {
                        watchManagerSeenStatus(resp.correlationId)
                    }
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Trade-in failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        btnStockTake?.setOnClickListener {
            val stockCorrelationId = WorkflowState.getStockCorrelationId(this)
            if (stockCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create stock unit first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val year = findViewById<EditText>(R.id.etYear).text?.toString()?.trim().orEmpty()
            val variant = findViewById<EditText>(R.id.etVariant).text?.toString()?.trim().orEmpty()
            val mileage = findViewById<EditText>(R.id.etMileage).text?.toString()?.trim().orEmpty()
            val stockNotes = findViewById<EditText>(R.id.etStockNotes).text?.toString()?.trim().orEmpty()
            val payload = JSONObject().apply {
                put("stockCorrelationId", stockCorrelationId)
                put("reason", "V1_STOCK_TAKE")
                put(
                    "vehicle",
                    JSONObject().apply {
                        put("registration", registration)
                        put("registrationNumber", registration)
                        put("licenceNumber", licenceNumber)
                        put("licenseNumber", licenceNumber)
                        put("make", make)
                        put("model", modelForValuation.ifBlank { model })
                        put("variant", variant)
                        put("year", year.ifBlank { firstRegYear })
                        put("mileage", mileage)
                        put("vin", vin)
                        put("engineNumber", engineNumber)
                        put("firstRegistrationDate", firstRegDate)
                        put("firstRegistrationYear", firstRegYear)
                        put("notes", stockNotes)
                    }
                )
                put(
                    "scan",
                    JSONObject().apply {
                        put("registration", registration)
                        put("licenseNumber", licenceNumber)
                        put("make", make)
                        put("model", modelForValuation.ifBlank { model })
                        put("variant", variant)
                        put("year", year.ifBlank { firstRegYear })
                        put("mileage", mileage)
                        put("vin", vin)
                    }
                )
                if (rawPayload.isNotBlank()) {
                    put("rawPayload", rawPayload)
                }
            }

            CommandApiService.createCommand(
                context = this,
                commandType = "STOCK_TAKE",
                payload = payload,
                onSuccess = { resp ->
                    runOnUiThread {
                        val msg = when {
                            resp.status.equals("pending_manager_approval", ignoreCase = true) ->
                                "Stock take submitted for manager approval: ${resp.correlationId}"
                            resp.status.equals("done", ignoreCase = true) -> "Stock take completed: ${resp.correlationId}"
                            else -> "Stock take queued: ${resp.correlationId}"
                        }
                        Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                    }
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Stock take failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }
    }

    private fun applyValuationResult(
        body: JSONObject,
        tvTradeValue: TextView?,
        tvRetailValue: TextView?,
        tvMarketValue: TextView?,
        tvValuationMeta: TextView?
    ) {
        val report = body.optJSONObject("report") ?: JSONObject()
        val trade = body.optLong("tradePrice", report.optLong("tradePrice", report.optLong("truetrade_tradePrice", 0L)))
        val retail = body.optLong("retailPrice", report.optLong("retailPrice", report.optLong("truetrade_retailPrice", 0L)))
        val market = body.optLong("marketPrice", report.optLong("marketPrice", report.optLong("truetrade_marketPrice", 0L)))
        val avgPrice = body.optLong("avg_price", report.optLong("avg_price", 0L))
        val highPrice = body.optLong("high_price", report.optLong("high_price", 0L))
        val lowPrice = body.optLong("low_price", report.optLong("low_price", 0L))
        val avgMileage = body.optLong("avg_mileage", report.optLong("avg_mileage", 0L))

        fun formatMoney(v: Long): String =
            if (v <= 0) "—" else "R %,d".format(v)

        tvTradeValue?.text = "Trade: ${formatMoney(trade)}"
        tvRetailValue?.text = "Retail: ${formatMoney(retail)}"
        tvMarketValue?.text = "Market: ${formatMoney(market)}"
        tvValuationMeta?.text =
            "Avg: ${formatMoney(avgPrice)}  ·  High: ${formatMoney(highPrice)}  ·  Low: ${formatMoney(lowPrice)}  ·  Avg km: ${if (avgMileage > 0) "%,d km".format(avgMileage) else "—"}"
    }

    private fun bindTradeInCaptureUi() {
        tvDamagesSummary = findViewById(R.id.tvDamagesSummary)
        damageWireframeView = findViewById(R.id.damageWireframeView)

        findViewById<MaterialButton>(R.id.btnCaptureFrontPhoto)?.setOnClickListener { startCameraCapture("front") }
        findViewById<MaterialButton>(R.id.btnCaptureLeftPhoto)?.setOnClickListener { startCameraCapture("left") }
        findViewById<MaterialButton>(R.id.btnCaptureRightPhoto)?.setOnClickListener { startCameraCapture("right") }
        findViewById<MaterialButton>(R.id.btnCaptureBackPhoto)?.setOnClickListener { startCameraCapture("back") }
        findViewById<MaterialButton>(R.id.btnClearDamages)?.setOnClickListener {
            damageEntries.clear()
            damageWireframeView?.clearMarkers()
            updateDamagesSummary()
        }
        findViewById<MaterialButton>(R.id.btnEditDamages)?.setOnClickListener {
            showEditDamagesDialog()
        }

        damageWireframeView?.setOnMarkerTappedListener { marker ->
            showAddDamageDialog(marker)
        }
        updateDamagesSummary()
    }

    private fun startCameraCapture(viewKey: String) {
        try {
            pendingCaptureView = viewKey
            val uri = createPhotoUri(viewKey)
            tradeInPhotoUris[viewKey] = uri
            takePictureLauncher.launch(uri)
        } catch (e: Exception) {
            Toast.makeText(this, "Unable to start camera: ${e.message}", Toast.LENGTH_LONG).show()
        }
    }

    private fun createPhotoUri(viewKey: String): Uri {
        val dir = File(cacheDir, "trade_in").apply { mkdirs() }
        val stamp = SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US).format(Date())
        val file = File(dir, "trade_${viewKey}_$stamp.jpg")
        return FileProvider.getUriForFile(this, "${packageName}.fileprovider", file)
    }

    private fun updatePhotoStatus(viewKey: String, uri: Uri?) {
        val statusViewId = when (viewKey) {
            "front" -> R.id.tvFrontPhotoStatus
            "left" -> R.id.tvLeftPhotoStatus
            "right" -> R.id.tvRightPhotoStatus
            "back" -> R.id.tvBackPhotoStatus
            else -> null
        } ?: return
        val text = if (uri == null) {
            "${viewKey.replaceFirstChar { it.uppercase() }}: not captured"
        } else {
            "${viewKey.replaceFirstChar { it.uppercase() }}: captured"
        }
        findViewById<TextView>(statusViewId)?.text = text
    }

    private fun showAddDamageDialog(marker: DamageMarker) {
        val container = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(40, 20, 40, 0)
        }
        val severityDefaults = linkedMapOf(
            "Minor" to Pair("Scratch / light cosmetic", 1200L),
            "Moderate" to Pair("Dent or paint damage", 3500L),
            "Major" to Pair("Panel replacement / major repair", 9000L)
        )
        var selectedSeverity = "Minor"
        val severityText = TextView(this).apply {
            text = "Severity preset: $selectedSeverity"
        }
        val etDescription = EditText(this).apply { hint = "Damage description (e.g. scratch)" }
        val etCost = EditText(this).apply {
            hint = "Estimated recon cost"
            inputType = android.text.InputType.TYPE_CLASS_NUMBER
        }
        val btnCycleSeverity = MaterialButton(this).apply {
            text = "Change severity preset"
            setOnClickListener {
                val keys = severityDefaults.keys.toList()
                val idx = keys.indexOf(selectedSeverity)
                selectedSeverity = keys[(idx + 1) % keys.size]
                severityText.text = "Severity preset: $selectedSeverity"
                val defaults = severityDefaults[selectedSeverity]!!
                if (etDescription.text.isNullOrBlank()) etDescription.setText(defaults.first)
                if (etCost.text.isNullOrBlank()) etCost.setText(defaults.second.toString())
            }
        }
        val initialDefaults = severityDefaults[selectedSeverity]!!
        etDescription.setText(initialDefaults.first)
        etCost.setText(initialDefaults.second.toString())
        container.addView(severityText)
        container.addView(btnCycleSeverity)
        container.addView(etDescription)
        container.addView(etCost)

        AlertDialog.Builder(this)
            .setTitle("Add damage (${marker.zone})")
            .setView(container)
            .setNegativeButton("Cancel", null)
            .setPositiveButton("Add") { _, _ ->
                val desc = etDescription.text?.toString()?.trim().orEmpty()
                val cost = etCost.text?.toString()?.trim()?.toLongOrNull()
                if (desc.isBlank() || cost == null || cost < 0L) {
                    Toast.makeText(this, "Enter description and a valid recon cost", Toast.LENGTH_LONG).show()
                    return@setPositiveButton
                }
                damageWireframeView?.addMarker(marker.xRatio, marker.yRatio, marker.zone)
                damageEntries += DamageEntry(marker, desc, cost, selectedSeverity)
                updateDamagesSummary()
            }
            .show()
    }

    private fun showEditDamagesDialog() {
        if (damageEntries.isEmpty()) {
            Toast.makeText(this, "No damage items to edit", Toast.LENGTH_SHORT).show()
            return
        }
        val labels = damageEntries.mapIndexed { idx, entry ->
            "#${idx + 1} ${entry.severity} • ${entry.marker.zone} • R %,d • ${entry.description}".format(entry.reconCost)
        }.toTypedArray()
        AlertDialog.Builder(this)
            .setTitle("Edit / delete damage")
            .setItems(labels) { _, which -> showEditSingleDamageDialog(which) }
            .setNegativeButton("Close", null)
            .show()
    }

    private fun showEditSingleDamageDialog(index: Int) {
        val entry = damageEntries.getOrNull(index) ?: return
        val container = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(40, 20, 40, 0)
        }
        val etSeverity = EditText(this).apply {
            hint = "Severity"
            setText(entry.severity)
        }
        val etDescription = EditText(this).apply {
            hint = "Description"
            setText(entry.description)
        }
        val etCost = EditText(this).apply {
            hint = "Recon cost"
            inputType = android.text.InputType.TYPE_CLASS_NUMBER
            setText(entry.reconCost.toString())
        }
        container.addView(etSeverity)
        container.addView(etDescription)
        container.addView(etCost)
        AlertDialog.Builder(this)
            .setTitle("Damage #${index + 1}")
            .setView(container)
            .setPositiveButton("Save") { _, _ ->
                val newSeverity = etSeverity.text?.toString()?.trim().orEmpty().ifBlank { "Minor" }
                val newDesc = etDescription.text?.toString()?.trim().orEmpty()
                val newCost = etCost.text?.toString()?.trim()?.toLongOrNull()
                if (newDesc.isBlank() || newCost == null || newCost < 0) {
                    Toast.makeText(this, "Enter valid description and cost", Toast.LENGTH_LONG).show()
                    return@setPositiveButton
                }
                entry.severity = newSeverity
                entry.description = newDesc
                entry.reconCost = newCost
                updateDamagesSummary()
            }
            .setNeutralButton("Delete") { _, _ ->
                damageEntries.removeAt(index)
                rebuildMarkersFromEntries()
                updateDamagesSummary()
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun rebuildMarkersFromEntries() {
        val copy = damageEntries.toList()
        damageWireframeView?.clearMarkers()
        copy.forEach { entry ->
            damageWireframeView?.addMarker(entry.marker.xRatio, entry.marker.yRatio, entry.marker.zone)
        }
    }

    private fun updateDamagesSummary() {
        if (damageEntries.isEmpty()) {
            tvDamagesSummary?.text = "No damages added"
            return
        }
        val total = damageEntries.sumOf { it.reconCost }
        val majors = damageEntries.count { it.severity.equals("Major", ignoreCase = true) }
        tvDamagesSummary?.text =
            "Damages: ${damageEntries.size}  •  Major: $majors  •  Estimated recon total: R %,d".format(total)
    }

    private fun watchManagerSeenStatus(correlationId: String) {
        Thread {
            repeat(30) {
                try {
                    Thread.sleep(2_000)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    if (status.managerSeen) {
                        runOnUiThread {
                            Toast.makeText(this, "Manager has seen this trade-in ✅", Toast.LENGTH_LONG).show()
                        }
                        return@Thread
                    }
                    if (status.status.equals("done", ignoreCase = true) || status.status.equals("rejected", ignoreCase = true)) {
                        return@Thread
                    }
                } catch (_: Exception) {
                    // Keep polling for a short time.
                }
            }
        }.start()
    }

    private fun focusAction(scrollView: NestedScrollView?, target: View?) {
        if (scrollView == null || target == null) return
        target.post {
            target.requestFocus()
            scrollView.smoothScrollTo(0, target.top)
            target.animate()
                .scaleX(1.04f)
                .scaleY(1.04f)
                .setDuration(140)
                .withEndAction {
                    target.animate().scaleX(1f).scaleY(1f).setDuration(140).start()
                }
                .start()
        }
    }

}