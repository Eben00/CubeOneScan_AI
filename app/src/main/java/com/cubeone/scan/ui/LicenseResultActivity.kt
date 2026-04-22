package com.cubeone.scan.ui

import android.content.Intent
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Color
import android.net.Uri
import android.os.Bundle
import android.util.LruCache
import android.util.Base64
import android.view.View
import android.view.LayoutInflater
import android.view.ViewGroup
import android.widget.EditText
import android.widget.ImageView
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.widget.NestedScrollView
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.R
import com.cubeone.scan.services.CommandApiService
import com.cubeone.scan.utils.WorkflowState
import com.google.android.material.button.MaterialButton
import com.google.android.material.card.MaterialCardView
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.Calendar
import java.util.Locale
import org.json.JSONObject

class LicenseResultActivity : AppCompatActivity() {
    private var selectedShareImageUri: Uri? = null
    private var selectedStock: CommandApiService.StockItem? = null
    private var activeTestDriveSessionId: String? = null
    private var senderDisplayName: String = ""
    private var senderDealershipName: String = ""
    private val thumbnailCache: LruCache<String, Bitmap> by lazy {
        val maxMemory = (Runtime.getRuntime().maxMemory() / 1024).toInt()
        val cacheSize = maxMemory / 32
        object : LruCache<String, Bitmap>(cacheSize) {
            override fun sizeOf(key: String, value: Bitmap): Int {
                return value.byteCount / 1024
            }
        }
    }

    private val pickShareImageLauncher = registerForActivityResult(ActivityResultContracts.GetContent()) { uri ->
        selectedShareImageUri = uri
        findViewById<TextView>(R.id.tvShareImageStatus)?.text =
            if (uri == null) "No photo attached" else "Photo attached"
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_license_result)
        senderDisplayName = AuthStore.getDisplayName(this).orEmpty()
            .ifBlank { AuthStore.getUserId(this).orEmpty().ifBlank { "Sales Consultant" } }
        senderDealershipName = AuthStore.getDealershipName(this).orEmpty()
            .ifBlank {
                AuthStore.getDealerId(this).orEmpty()
                    .ifBlank { "Dealership" }
                    .let { "Dealer $it" }
            }

        val surname = intent.getStringExtra("SURNAME").orEmpty()
        val initials = intent.getStringExtra("NAMES").orEmpty()
        val idNumber = intent.getStringExtra("ID_NUMBER").orEmpty()
        val licenseNumber = intent.getStringExtra("LICENSE_NUMBER").orEmpty()
        val validFrom = intent.getStringExtra("ISSUE_DATE").orEmpty()
        val validTo = intent.getStringExtra("EXPIRY_DATE").orEmpty()
        val vehicleCodes = intent.getStringExtra("VEHICLE_CODES").orEmpty()
        val photoBase64 = intent.getStringExtra("PHOTO")
        val postScanAction = intent.getStringExtra("POST_SCAN_ACTION").orEmpty()
        val gender = intent.getStringExtra("GENDER").orEmpty()
        val dobFromCard = intent.getStringExtra("DOB").orEmpty()

        if (idNumber.isEmpty()) {
            Toast.makeText(this, "License scan incomplete: ID number missing", Toast.LENGTH_LONG).show()
        }

        findViewById<TextView>(R.id.tvSurname).text =
            getString(R.string.label_surname, surname.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvInitials).text =
            getString(R.string.label_first_names, initials.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvGender).text =
            getString(R.string.label_gender, gender.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvIdNumber).text =
            getString(R.string.label_id_number, idNumber.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvLicenseNumber).text =
            getString(R.string.label_license_number, licenseNumber.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvValidFrom).text =
            getString(R.string.label_issue_date, validFrom.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvValidTo).text =
            getString(R.string.label_expiry_date, validTo.ifBlank { getString(R.string.unknown) })
        findViewById<TextView>(R.id.tvVehicleCodes).text =
            getString(R.string.label_vehicle_codes, vehicleCodes.ifBlank { getString(R.string.unknown) })

        val tvBirthDate = findViewById<TextView>(R.id.tvBirthDate)
        val tvDobCard = findViewById<TextView>(R.id.tvDobCard)

        if (idNumber.length >= 6) {
            try {
                val yearPrefix = if (idNumber.substring(0, 2).toInt() < 50) "20" else "19"
                val birthYear = yearPrefix + idNumber.substring(0, 2)
                val birthMonth = idNumber.substring(2, 4)
                val birthDay = idNumber.substring(4, 6)
                tvBirthDate.text = getString(R.string.dob_format, "$birthDay/$birthMonth/$birthYear")
            } catch (_: Exception) {
                tvBirthDate.text = getString(R.string.dob_format, getString(R.string.unknown))
            }
        } else {
            tvBirthDate.text = getString(R.string.dob_format, getString(R.string.unknown))
        }

        if (dobFromCard.isNotBlank()) {
            tvDobCard.visibility = View.VISIBLE
            tvDobCard.text = getString(R.string.dob_on_card_format, dobFromCard)
        }

        if (!photoBase64.isNullOrEmpty()) {
            try {
                val imageBytes = Base64.decode(photoBase64, Base64.DEFAULT)
                val bitmap = BitmapFactory.decodeByteArray(imageBytes, 0, imageBytes.size)
                findViewById<ImageView>(R.id.imgPhoto).setImageBitmap(bitmap)
            } catch (_: Exception) {
                // Some licences omit or encode photo differently
            }
        }

        findViewById<MaterialButton>(R.id.btnDone).setOnClickListener {
            finish()
        }

        when (postScanAction) {
            "share_lead" -> {
                Toast.makeText(this, "Driver captured. Create lead, then tap Share Lead.", Toast.LENGTH_LONG).show()
                focusAction(findViewById(R.id.svLicenseRoot), findViewById(R.id.btnCreateLead))
            }
            "share_stock" -> {
                Toast.makeText(this, "Driver captured. Create lead, pick stock, then tap Share Stock Unit.", Toast.LENGTH_LONG).show()
                focusAction(findViewById(R.id.svLicenseRoot), findViewById(R.id.btnPickStock))
            }
        }

        findViewById<MaterialButton>(R.id.btnCreateLead).setOnClickListener {
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim()
            val normalizedCellphone = cellphone.replace(" ", "")
            if (normalizedCellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val driverLicense = JSONObject().apply {
                put("firstName", initials)
                put("surname", surname)
                put("idNumber", idNumber)
                put("licenseNumber", licenseNumber)
                put("dateOfBirth", dobFromCard)
                put("validFrom", validFrom)
                put("expiryDate", validTo)
                put("gender", gender)
                put("phone", normalizedCellphone)
            }
            val payload = JSONObject().apply {
                put("driverLicense", driverLicense)
                put("phone", normalizedCellphone)
                put(
                    "createdBy",
                    JSONObject().apply {
                        put("userId", AuthStore.getUserId(this@LicenseResultActivity).orEmpty())
                        put("name", AuthStore.getDisplayName(this@LicenseResultActivity).orEmpty())
                        put("email", AuthStore.getUserEmail(this@LicenseResultActivity).orEmpty())
                        put("role", AuthStore.getRole(this@LicenseResultActivity).orEmpty())
                    }
                )
            }
            CommandApiService.createCommand(
                context = this,
                commandType = "CREATE_LEAD",
                payload = payload,
                onSuccess = { resp ->
                    WorkflowState.setLeadCorrelationId(this, resp.correlationId)
                    runOnUiThread {
                        Toast.makeText(
                            this,
                            getString(R.string.toast_lead_queued, resp.correlationId),
                            Toast.LENGTH_LONG
                        ).show()
                    }
                    watchLeadCommandCompletion(resp.correlationId)
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Create lead failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnShareLead).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val payload = JSONObject().apply {
                put("leadCorrelationId", leadCorrelationId)
                put("target", "EvolveSA")
            }
            CommandApiService.createCommand(
                context = this,
                commandType = "SHARE_LEAD",
                payload = payload,
                onSuccess = { resp ->
                    runOnUiThread {
                        Toast.makeText(
                            this,
                            getString(R.string.toast_share_queued, resp.correlationId),
                            Toast.LENGTH_LONG
                        ).show()
                    }
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Share lead failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnQuickCreditCheck).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val hasConsent = findViewById<android.widget.CheckBox>(R.id.cbCreditConsent).isChecked
            if (!hasConsent) {
                Toast.makeText(this, "Capture consent before running credit check", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim()
            val normalizedCellphone = cellphone.replace(" ", "")
            if (normalizedCellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val email = findViewById<EditText>(R.id.etCreditEmail).text?.toString().orEmpty().trim()
            val payload = JSONObject().apply {
                put("leadCorrelationId", leadCorrelationId)
                put(
                    "consent",
                    JSONObject().apply {
                        put("accepted", true)
                        put("capturedAt", System.currentTimeMillis())
                        put("capturedByUserId", AuthStore.getUserId(this@LicenseResultActivity).orEmpty())
                    }
                )
                put(
                    "applicant",
                    JSONObject().apply {
                        put("firstName", initials)
                        put("surname", surname)
                        put("idNumber", idNumber)
                        put("mobile", normalizedCellphone)
                        put("email", email)
                    }
                )
            }
            CommandApiService.createCommand(
                context = this,
                commandType = "CREDIT_CHECK",
                payload = payload,
                onSuccess = { resp ->
                    runOnUiThread {
                        Toast.makeText(this, "Credit check queued. Ref: ${resp.correlationId}", Toast.LENGTH_LONG).show()
                    }
                    watchCreditCheckCompletion(resp.correlationId)
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Credit check failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnStartTestDrive).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val vehicleRef = findViewById<EditText>(R.id.etTestDriveVehicleRef).text?.toString().orEmpty().trim()
            if (vehicleRef.isBlank()) {
                Toast.makeText(this, "Enter vehicle ref", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim().replace(" ", "")
            if (cellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val emergency = findViewById<EditText>(R.id.etTestDriveEmergencyMobile).text?.toString().orEmpty().trim()
            val returnMinutes = findViewById<EditText>(R.id.etPlannedReturnMinutes).text?.toString().orEmpty().trim().toIntOrNull() ?: 30
            val cal = Calendar.getInstance()
            cal.add(Calendar.MINUTE, returnMinutes.coerceIn(5, 180))
            val payload = JSONObject().apply {
                put("leadId", leadCorrelationId)
                put("vehicleRef", vehicleRef)
                put("driverIdNumber", idNumber)
                put("mobile", cellphone)
                put("emergencyMobile", emergency)
                put("plannedReturnAt", java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US).apply {
                    timeZone = java.util.TimeZone.getTimeZone("UTC")
                }.format(cal.time))
            }
            CommandApiService.startTestDrive(
                context = this,
                payload = payload,
                onSuccess = { json ->
                    val session = json.optJSONObject("session")
                    activeTestDriveSessionId = session?.optString("sessionId").orEmpty().ifBlank { null }
                    runOnUiThread {
                        findViewById<TextView>(R.id.tvTestDriveStatus).text =
                            "Active session: ${activeTestDriveSessionId ?: "-"}"
                        Toast.makeText(this, "Test drive started", Toast.LENGTH_LONG).show()
                    }
                    refreshActiveTestDriveStatus()
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Start test drive failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnTestDriveCheckin).setOnClickListener {
            val sessionId = activeTestDriveSessionId
            if (sessionId.isNullOrBlank()) {
                Toast.makeText(this, "Start a test drive first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            CommandApiService.testDriveCheckin(
                context = this,
                sessionId = sessionId,
                payload = JSONObject().apply { put("note", "Salesman safety check-in") },
                onSuccess = {
                    runOnUiThread { Toast.makeText(this, "Check-in captured", Toast.LENGTH_SHORT).show() }
                    refreshActiveTestDriveStatus()
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Check-in failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnCompleteTestDrive).setOnClickListener {
            val sessionId = activeTestDriveSessionId
            if (sessionId.isNullOrBlank()) {
                Toast.makeText(this, "No active test drive", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            CommandApiService.completeTestDrive(
                context = this,
                sessionId = sessionId,
                payload = JSONObject().apply { put("returnNotes", "Vehicle returned safely") },
                onSuccess = {
                    activeTestDriveSessionId = null
                    runOnUiThread {
                        findViewById<TextView>(R.id.tvTestDriveStatus).text = getString(R.string.test_drive_status_placeholder)
                        Toast.makeText(this, "Test drive completed", Toast.LENGTH_SHORT).show()
                    }
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Complete failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnShareStockUnit).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val selected = selectedStock
            if (selected == null) {
                Toast.makeText(this, "Pick stock from dealer list first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            val stockLink = findViewById<EditText>(R.id.etShareLink).text?.toString()?.trim().orEmpty()
            val photoUrl = findViewById<EditText>(R.id.etSharePhotoUrl).text?.toString()?.trim().orEmpty()
            val note = findViewById<EditText>(R.id.etShareNote).text?.toString()?.trim().orEmpty()
            Thread {
                val resolvedPhotoUrl = if (photoUrl.isNotBlank()) photoUrl else resolveStockImageUrl(selected)
                val effectiveImageUri = selectedShareImageUri ?: downloadImageToCacheUri(resolvedPhotoUrl)
                val message = buildStockShareMessage(
                    stockUnitId = selected.stockNumber.ifBlank { selected.registrationNumber.ifBlank { "stock_selected" } },
                    make = selected.make,
                    model = selected.model,
                    registration = selected.registrationNumber,
                    year = selected.year,
                    price = selected.price,
                    link = stockLink,
                    photoUrl = resolvedPhotoUrl,
                    note = note
                )
                runOnUiThread {
                    if (selectedShareImageUri == null && effectiveImageUri != null) {
                        findViewById<TextView>(R.id.tvShareImageStatus)?.text = "Using first stock photo"
                    }
                    launchShareChooser(message, effectiveImageUri)
                }
            }.start()
        }

        findViewById<MaterialButton>(R.id.btnAttachShareImage).setOnClickListener {
            pickShareImageLauncher.launch("image/*")
        }
        findViewById<MaterialButton>(R.id.btnPickStock).setOnClickListener {
            val q = findViewById<EditText>(R.id.etStockSearch).text?.toString()?.trim().orEmpty()
            loadAndPickStock(forceRefresh = false, search = q, openPicker = true)
        }
        findViewById<MaterialButton>(R.id.btnRefreshStocks).setOnClickListener {
            val q = findViewById<EditText>(R.id.etStockSearch).text?.toString()?.trim().orEmpty()
            loadAndPickStock(forceRefresh = true, search = q, openPicker = false)
        }
        loadAndPickStock(forceRefresh = false, search = "", openPicker = false)
        refreshActiveTestDriveStatus()
    }

    private fun refreshActiveTestDriveStatus() {
        CommandApiService.getActiveTestDrives(
            context = this,
            onSuccess = { json ->
                val arr = json.optJSONArray("sessions")
                var statusText = getString(R.string.test_drive_status_placeholder)
                if (arr != null && arr.length() > 0) {
                    val first = arr.optJSONObject(0)
                    val sid = first?.optString("sessionId").orEmpty()
                    val safety = first?.optString("safetyStatus").orEmpty().ifBlank { "on_track" }
                    val overdue = first?.optInt("overdueMinutes", 0) ?: 0
                    if (sid.isNotBlank()) activeTestDriveSessionId = sid
                    statusText = if (overdue > 0) {
                        "Safety alert: $safety ($overdue min overdue)"
                    } else {
                        "Active test drive: $safety"
                    }
                }
                runOnUiThread {
                    val view = findViewById<TextView>(R.id.tvTestDriveStatus)
                    view.text = statusText
                    val danger = statusText.contains("overdue", ignoreCase = true)
                    view.setTextColor(if (danger) Color.parseColor("#B3261E") else ContextCompat.getColor(this, R.color.text_secondary))
                }
            },
            onError = { err ->
                runOnUiThread { findViewById<TextView>(R.id.tvTestDriveStatus).text = "Test drive status unavailable: $err" }
            }
        )
    }

    private fun watchLeadCommandCompletion(correlationId: String) {
        Thread {
            repeat(8) {
                try {
                    Thread.sleep(1500)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    val s = status.status.lowercase()
                    if (s == "done") {
                        val leadId = status.result?.optString("leadId").orEmpty()
                        runOnUiThread {
                            val msg = if (leadId.isNotBlank()) {
                                "Lead synced successfully (ID: $leadId)"
                            } else {
                                "Lead synced successfully"
                            }
                            Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                        }
                        return@Thread
                    }
                    if (s == "failed") {
                        runOnUiThread {
                            Toast.makeText(
                                this,
                                "Lead failed after queue: ${status.error ?: "Unknown error"}",
                                Toast.LENGTH_LONG
                            ).show()
                        }
                        return@Thread
                    }
                } catch (_: Exception) {
                    // Keep polling; connector may still be processing.
                }
            }
        }.start()
    }

    private fun watchCreditCheckCompletion(correlationId: String) {
        Thread {
            repeat(10) {
                try {
                    Thread.sleep(1500)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    val s = status.status.lowercase()
                    if (s == "done") {
                        val cc = status.result?.optJSONObject("creditCheck")
                        val score = cc?.optInt("score")
                        val band = cc?.optString("band").orEmpty()
                        val decision = cc?.optString("decision").orEmpty()
                        runOnUiThread {
                            val text = if (score != null && score >= 0) {
                                "Quick score: $score (${band.ifBlank { "unknown" }}) • decision: ${decision.ifBlank { "review" }}"
                            } else {
                                "Credit check completed"
                            }
                            findViewById<TextView>(R.id.tvCreditCheckResult).text = text
                            Toast.makeText(this, text, Toast.LENGTH_LONG).show()
                        }
                        return@Thread
                    }
                    if (s == "failed") {
                        runOnUiThread {
                            val msg = "Credit check failed: ${status.error ?: "Unknown error"}"
                            findViewById<TextView>(R.id.tvCreditCheckResult).text = msg
                            Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                        }
                        return@Thread
                    }
                } catch (_: Exception) {
                    // keep polling
                }
            }
        }.start()
    }

    private fun buildStockShareMessage(
        stockUnitId: String,
        make: String,
        model: String,
        registration: String,
        year: String,
        price: String,
        link: String,
        photoUrl: String,
        note: String
    ): String {
        val lines = mutableListOf<String>()
        lines += "Stock unit available"
        lines += "Stock ID: $stockUnitId"
        if (make.isNotBlank() || model.isNotBlank()) lines += "Vehicle: ${make.trim()} ${model.trim()}".trim()
        if (registration.isNotBlank()) lines += "Registration: $registration"
        if (year.isNotBlank()) lines += "Year: $year"
        if (price.isNotBlank()) lines += "Price: $price"
        if (link.isNotBlank()) lines += "Link: $link"
        // Only include a photo URL when we could not attach an actual image file.
        if (photoUrl.isNotBlank() && selectedShareImageUri == null) lines += "Photo: $photoUrl"
        if (note.isNotBlank()) {
            lines += ""
            lines += note
        }
        lines += ""
        lines += "Sent by: $senderDisplayName"
        lines += "Dealership: $senderDealershipName"
        return lines.joinToString("\n")
    }

    private fun launchShareChooser(message: String, imageUri: Uri?) {
        val sendIntent = Intent(Intent.ACTION_SEND).apply {
            putExtra(Intent.EXTRA_TEXT, message)
            if (imageUri != null) {
                type = "image/*"
                putExtra(Intent.EXTRA_STREAM, imageUri)
                addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION)
            } else {
                type = "text/plain"
            }
        }
        startActivity(Intent.createChooser(sendIntent, "Share stock unit via"))
    }

    private fun downloadImageToCacheUri(url: String): Uri? {
        val source = url.trim()
        if (source.isBlank() || !(source.startsWith("http://") || source.startsWith("https://"))) return null
        return try {
            val dir = File(cacheDir, "share_images").apply { mkdirs() }
            val file = File(dir, "stock_first_photo_${System.currentTimeMillis()}.jpg")
            URL(source).openStream().use { input ->
                FileOutputStream(file).use { output -> input.copyTo(output) }
            }
            FileProvider.getUriForFile(this, "${packageName}.fileprovider", file)
        } catch (_: Exception) {
            null
        }
    }

    private fun loadAndPickStock(
        forceRefresh: Boolean,
        search: String,
        openPicker: Boolean,
        retryOnError: Boolean = true
    ) {
        CommandApiService.getStocks(
            context = this,
            search = search,
            refresh = forceRefresh,
            mode = "stock_take",
            onSuccess = { response ->
                runOnUiThread {
                    val items = response.stocks
                    findViewById<TextView>(R.id.tvStockLastUpdated)?.text =
                        "Stock list last updated: ${response.cachedAt ?: "—"} (${response.provider ?: response.source ?: "unknown"})"
                    if (items.isEmpty()) {
                        if (openPicker) {
                            val warning = response.warning?.trim().orEmpty()
                            val message = if (warning.isNotBlank()) {
                                "No stock available: $warning"
                            } else {
                                "No stock available for dealer ${response.dealerScope ?: "unknown"}"
                            }
                            Toast.makeText(this, message, Toast.LENGTH_LONG).show()
                        }
                        return@runOnUiThread
                    }
                    if (openPicker) showStockPicker(items)
                }
            },
            onError = { err ->
                runOnUiThread {
                    val shouldRetry =
                        retryOnError && !forceRefresh && (
                            err.contains("HTTP 5") ||
                                err.contains("timeout", ignoreCase = true) ||
                                err.contains("failed to", ignoreCase = true)
                            )
                    if (shouldRetry) {
                        loadAndPickStock(
                            forceRefresh = false,
                            search = search,
                            openPicker = openPicker,
                            retryOnError = false
                        )
                        return@runOnUiThread
                    }
                    Toast.makeText(this, "Load stock failed: $err", Toast.LENGTH_LONG).show()
                }
            }
        )
    }

    private fun showStockPicker(items: List<CommandApiService.StockItem>) {
        if (items.isEmpty()) return

        val dialogView = LayoutInflater.from(this).inflate(R.layout.dialog_stock_picker, null)
        val recyclerView = dialogView.findViewById<RecyclerView>(R.id.rvStockPicker)
        recyclerView.layoutManager = LinearLayoutManager(this)
        val dialog = AlertDialog.Builder(this)
            .setView(dialogView)
            .setNegativeButton("Cancel", null)
            .create()

        recyclerView.adapter = StockPickerAdapter(items) { selected ->
            selectedStock = selected
            findViewById<TextView>(R.id.tvSelectedStock)?.text = "Selected: ${selected.label()}"
            val preferredImageUrl = resolveStockImageUrl(selected)
            if (findViewById<EditText>(R.id.etSharePhotoUrl).text.isNullOrBlank() && preferredImageUrl.isNotBlank()) {
                findViewById<EditText>(R.id.etSharePhotoUrl).setText(preferredImageUrl)
            }
            dialog.dismiss()
        }

        dialog.show()
    }

    private fun resolveStockImageUrl(stock: CommandApiService.StockItem): String {
        val raw = stock.raw
        val imageUrls = raw?.optJSONArray("imageUrls")
        if (imageUrls != null) {
            for (i in 0 until imageUrls.length()) {
                val candidate = imageUrls.optString(i).trim()
                if (candidate.startsWith("http://") || candidate.startsWith("https://")) return candidate
            }
        }
        return stock.primaryImageUrl?.trim().orEmpty()
    }

    private fun formatStockPriceLine(stock: CommandApiService.StockItem): String {
        fun valueOrDash(v: String): String = v.trim().ifBlank { "—" }
        val autoTraderPrice = stock.autoTraderPrice.trim().ifBlank { stock.price.trim() }
        return "AT: ${valueOrDash(autoTraderPrice)} • Trade: ${valueOrDash(stock.tradePrice)} • Retail: ${valueOrDash(stock.retailPrice)} • Market: ${valueOrDash(stock.marketPrice)}"
    }

    private fun formatStockRegLine(stock: CommandApiService.StockItem): String {
        return if (stock.registrationNumber.isBlank()) "Reg: —" else "Reg: ${stock.registrationNumber}"
    }

    private fun loadThumbnailInto(view: ImageView, stock: CommandApiService.StockItem) {
        val url = resolveStockImageUrl(stock)
        if (url.isBlank()) {
            view.setImageDrawable(null)
            view.setBackgroundColor(ContextCompat.getColor(this, R.color.photo_placeholder))
            return
        }
        val cached = thumbnailCache.get(url)
        if (cached != null) {
            view.setImageBitmap(cached)
            return
        }
        view.setImageDrawable(null)
        view.setBackgroundColor(ContextCompat.getColor(this, R.color.photo_placeholder))
        Thread {
            try {
                val bmp = BitmapFactory.decodeStream(URL(url).openStream())
                if (bmp != null) {
                    thumbnailCache.put(url, bmp)
                    runOnUiThread {
                        if (view.tag == url) {
                            view.setImageBitmap(bmp)
                        }
                    }
                }
            } catch (_: Exception) {
            }
        }.start()
        view.tag = url
    }

    private inner class StockPickerAdapter(
        private val items: List<CommandApiService.StockItem>,
        private val onSelected: (CommandApiService.StockItem) -> Unit
    ) : RecyclerView.Adapter<StockPickerAdapter.StockViewHolder>() {

        inner class StockViewHolder(view: View) : RecyclerView.ViewHolder(view) {
            val card: MaterialCardView = view as MaterialCardView
            val thumb: ImageView = view.findViewById(R.id.ivStockThumb)
            val title: TextView = view.findViewById(R.id.tvStockTitle)
            val priceReg: TextView = view.findViewById(R.id.tvStockPriceReg)
            val desc: TextView = view.findViewById(R.id.tvStockDescPreview)
        }

        override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): StockViewHolder {
            val view = LayoutInflater.from(parent.context)
                .inflate(R.layout.item_stock_picker, parent, false)
            return StockViewHolder(view)
        }

        override fun onBindViewHolder(holder: StockViewHolder, position: Int) {
            val item = items[position]
            holder.title.text = item.label()
            holder.priceReg.text = "${formatStockPriceLine(item)} • ${formatStockRegLine(item)}"
            val descText = item.raw?.optString("description").orEmpty().trim()
            holder.desc.text = if (descText.isBlank()) "" else descText
            loadThumbnailInto(holder.thumb, item)
            holder.card.setOnClickListener { onSelected(item) }
        }

        override fun getItemCount(): Int = items.size
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
