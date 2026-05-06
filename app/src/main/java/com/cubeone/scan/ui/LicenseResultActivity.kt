package com.cubeone.scan.ui

import android.content.Intent
import android.graphics.Bitmap
import android.graphics.BitmapFactory
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
import androidx.appcompat.content.res.AppCompatResources
import androidx.core.content.ContextCompat
import androidx.core.content.FileProvider
import androidx.core.widget.NestedScrollView
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.cubeone.scan.BuildConfig
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.R
import com.cubeone.scan.services.CommandApiService
import com.cubeone.scan.utils.WorkflowState
import com.google.android.material.button.MaterialButton
import com.google.android.material.card.MaterialCardView
import com.google.android.material.textfield.TextInputLayout
import com.google.android.material.progressindicator.CircularProgressIndicator
import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.util.Locale
import org.json.JSONObject

class LicenseResultActivity : AppCompatActivity() {
    private data class CreditBand(val label: String, val range: String, val colorResId: Int)
    private companion object {
        const val COMM_PREFS = "lead_comm_log"
        const val COMM_LAST_TEXT = "last_text"
        /** Polling cadence × rounds ≈ 4 min — customer approval often happens after backgrounding the app. */
        const val CONSENT_POLL_MAX_ROUNDS = 96
        /** Credit checks can take longer on cloud latency / provider load; poll up to ~90s. */
        const val CREDIT_CHECK_POLL_MAX_ROUNDS = 60
    }
    private var selectedShareImageUri: Uri? = null
    private var selectedStock: CommandApiService.StockItem? = null
    private var creditConsentId: String? = null
    private var creditConsentStatus: String = ""
    private val consentPollLock = Any()
    private var consentPollRunning = false
    private var consentEmailFailureToastShownFor: String? = null
    private var consentEmailSentToastShownFor: String? = null
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
        renderLastCommunication()
        renderCreditScoreUi(null, null)
        renderCreditConsentStatus("")
        val emailConsentFlow = BuildConfig.ENABLE_EMAIL_CONSENT_FLOW
        findViewById<TextInputLayout>(R.id.tilCreditEmail).visibility =
            if (emailConsentFlow) View.VISIBLE else View.GONE
        findViewById<MaterialButton>(R.id.btnRequestCreditConsent).visibility =
            if (emailConsentFlow) View.VISIBLE else View.GONE
        findViewById<TextView>(R.id.tvCreditConsentStatus).visibility =
            if (emailConsentFlow) View.VISIBLE else View.GONE
        if (!emailConsentFlow) {
            findViewById<MaterialButton>(R.id.btnQuickCreditCheck).isEnabled = true
        }
        restoreSavedCreditConsentState()

        when (postScanAction) {
            "share_lead" -> {
                Toast.makeText(this, "Driver captured. Create lead, then tap Share Lead.", Toast.LENGTH_LONG).show()
                focusAction(findViewById(R.id.svLicenseRoot), findViewById(R.id.btnCreateLead))
            }
            "share_stock" -> {
                Toast.makeText(this, "Driver captured. Create lead, pick stock, then tap Share Stock Unit.", Toast.LENGTH_LONG).show()
                focusAction(findViewById(R.id.svLicenseRoot), findViewById(R.id.btnPickStock))
            }
            "test_drive" -> {
                Toast.makeText(this, "Driver captured for Test Drive workflow.", Toast.LENGTH_LONG).show()
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
                    queueCommunicationLog(
                        note = "Lead shared from mobile app to EvolveSA target.",
                        channel = "share_lead",
                        leadCorrelationId = leadCorrelationId
                    )
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Share lead failed: $err", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnLogCommunication).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val note = findViewById<EditText>(R.id.etCommunicationNote).text?.toString().orEmpty().trim()
            if (note.isBlank()) {
                Toast.makeText(this, "Enter a note first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            queueCommunicationLog(
                note = note,
                channel = "manual_note",
                leadCorrelationId = leadCorrelationId
            )
        }

        findViewById<MaterialButton>(R.id.btnRequestCreditConsent).setOnClickListener {
            if (!BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) return@setOnClickListener
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val hasConsent = findViewById<android.widget.CheckBox>(R.id.cbCreditConsent).isChecked
            if (!hasConsent) {
                Toast.makeText(this, "Capture consent before requesting approval email", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim()
            val normalizedCellphone = cellphone.replace(" ", "")
            if (normalizedCellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val email = findViewById<EditText>(R.id.etCreditEmail).text?.toString().orEmpty().trim()
            if (!email.contains("@")) {
                Toast.makeText(this, getString(R.string.credit_consent_email_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val payload = JSONObject().apply {
                put("leadCorrelationId", leadCorrelationId)
                put("purpose", "soft_credit_check_affordability")
                put("channel", "email_link")
                put("noticeVersion", "credit_consent_v1_2026-04")
                put("expiresInHours", 24)
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
            CommandApiService.createConsent(
                context = this,
                payload = payload,
                onSuccess = { resp ->
                    WorkflowState.setEmailConsentIdForLead(this, leadCorrelationId, resp.consentId)
                    creditConsentId = resp.consentId
                    creditConsentStatus = resp.status
                    val reusedPending = resp.raw.optBoolean("reusedPendingConsent")
                    runOnUiThread {
                        renderCreditConsentStatus(resp.status)
                        val delivery = resp.raw.optJSONObject("delivery")
                        val dispatch =
                            delivery?.optString("emailDispatch")?.lowercase(Locale.getDefault()).orEmpty()
                        val toastText = when {
                            reusedPending -> getString(R.string.credit_consent_already_pending, resp.consentId)
                            dispatch == "failed" -> getString(
                                R.string.credit_consent_email_failed,
                                delivery?.optString("warning").orEmpty().ifBlank { "unknown" }
                            )
                            dispatch == "pending" || dispatch.isEmpty() -> getString(
                                R.string.credit_consent_email_dispatching,
                                resp.consentId
                            )
                            else -> getString(R.string.credit_consent_request_sent, resp.consentId)
                        }
                        Toast.makeText(this, toastText, Toast.LENGTH_LONG).show()
                    }
                    watchConsentStatus(resp.consentId)
                },
                onError = { err ->
                    runOnUiThread {
                        Toast.makeText(this, "Consent request failed: $err", Toast.LENGTH_LONG).show()
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
            val resolvedEmailConsentId =
                if (BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) {
                    val cid = resolveConsentIdForCreditCheckRun(leadCorrelationId)?.trim().orEmpty()
                    if (cid.isEmpty()) {
                        Toast.makeText(this, getString(R.string.credit_consent_id_missing_for_run), Toast.LENGTH_LONG)
                            .show()
                        return@setOnClickListener
                    }
                    cid
                } else null
            val payload = JSONObject().apply {
                put("leadCorrelationId", leadCorrelationId)
                if (BuildConfig.ENABLE_EMAIL_CONSENT_FLOW && resolvedEmailConsentId != null) {
                    put("consentId", resolvedEmailConsentId)
                } else if (!BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) {
                    put(
                        "consent",
                        JSONObject().apply {
                            put("accepted", true)
                            put("capturedAt", System.currentTimeMillis())
                            put("capturedByUserId", AuthStore.getUserId(this@LicenseResultActivity).orEmpty())
                        }
                    )
                }
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
                    WorkflowState.getLeadCorrelationId(this)?.let { leadCorrelationId ->
                        queueCommunicationLog(
                            note = "Stock unit shared from mobile app via external channel.",
                            channel = "share_stock_unit",
                            leadCorrelationId = leadCorrelationId
                        )
                    }
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
    }

    override fun onResume() {
        super.onResume()
        refreshCreditConsentUiFromConnector()
    }

    /** In-memory consent id or the one persisted for this scan session’s lead (rotation / process death). */
    private fun resolveConsentIdForCreditCheckRun(leadCorrelationId: String?): String? {
        val fromMem = creditConsentId?.trim().orEmpty()
        if (fromMem.isNotEmpty()) return fromMem
        val lead = leadCorrelationId?.trim().orEmpty()
        if (lead.isEmpty()) return null
        return WorkflowState.getEmailConsentIdForLead(this, lead)?.trim()?.takeIf { it.isNotEmpty() }
    }

    /** Customer may approve while we are in Gmail / Chrome; foregrounding must re-fetch connector state. */
    private fun refreshCreditConsentUiFromConnector() {
        if (!BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) return
        val leadId = WorkflowState.getLeadCorrelationId(this)?.trim().orEmpty()
        val cid = creditConsentId?.trim().takeUnless { it.isNullOrEmpty() }
            ?: if (leadId.isEmpty()) "" else WorkflowState.getEmailConsentIdForLead(this, leadId)?.trim().orEmpty()
        if (cid.isEmpty()) return
        creditConsentId = cid
        CommandApiService.getConsentStatus(
            context = this,
            consentId = cid,
            onSuccess = { resp ->
                val sid = resp.consentId.ifBlank { cid }
                creditConsentId = sid
                creditConsentStatus = resp.status.lowercase(Locale.getDefault())
                if (leadId.isNotEmpty()) {
                    WorkflowState.setEmailConsentIdForLead(this, leadId, sid)
                }
                runOnUiThread {
                    renderCreditConsentStatus(resp.status)
                }
                if (resp.status.lowercase(Locale.getDefault()) == "pending") {
                    watchConsentStatus(sid)
                }
            },
            onError = { err ->
                // Deployed connector may prune/miss old consent records after restarts.
                // If a persisted consentId no longer exists, clear local state so user can request a new one.
                val normalized = err.lowercase(Locale.getDefault())
                if (normalized.contains("consent_not_found") || normalized.contains("http 404")) {
                    creditConsentId = null
                    creditConsentStatus = "none"
                    if (leadId.isNotEmpty()) {
                        WorkflowState.clearEmailConsentIdForLead(this, leadId)
                    }
                    runOnUiThread {
                        renderCreditConsentStatus("none")
                    }
                }
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
            var completed = false
            repeat(CREDIT_CHECK_POLL_MAX_ROUNDS) {
                try {
                    Thread.sleep(1500)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    val s = status.status.lowercase()
                    if (s == "done") {
                        val cc = status.result?.optJSONObject("creditCheck")
                        val scoreFromCreditCheck = cc?.optInt("score", -1) ?: -1
                        val scoreFromRoot = status.result?.optInt("score", -1) ?: -1
                        val score = when {
                            scoreFromCreditCheck >= 0 -> scoreFromCreditCheck
                            scoreFromRoot >= 0 -> scoreFromRoot
                            else -> null
                        }
                        runOnUiThread {
                            val resultView = findViewById<TextView>(R.id.tvCreditCheckResult)
                            val text = if (score != null && score >= 0) {
                                val creditBand = resolveCreditBand(score)
                                val scoreText = getString(R.string.credit_score_result_format, score)
                                val ratingText = getString(
                                    R.string.credit_score_rating_format,
                                    creditBand.label,
                                    creditBand.range
                                )
                                renderCreditScoreUi(score, creditBand)
                                resultView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
                                "$scoreText\n$ratingText"
                            } else {
                                renderCreditScoreUi(null, null)
                                resultView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
                                getString(R.string.credit_score_unknown)
                            }
                            resultView.text = text
                            Toast.makeText(this, text, Toast.LENGTH_LONG).show()
                        }
                        completed = true
                        return@Thread
                    }
                    if (s == "failed") {
                        runOnUiThread {
                            val msg = "Credit check failed: ${status.error ?: "Unknown error"}"
                            val resultView = findViewById<TextView>(R.id.tvCreditCheckResult)
                            resultView.text = msg
                            resultView.setTextColor(ContextCompat.getColor(this, R.color.credit_band_unfavourable))
                            renderCreditScoreUi(null, null)
                            Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                        }
                        completed = true
                        return@Thread
                    }
                } catch (_: Exception) {
                    // keep polling
                }
            }
            if (!completed) {
                runOnUiThread {
                    Toast.makeText(
                        this,
                        "Credit check is still processing. Please wait a little longer and try again.",
                        Toast.LENGTH_LONG
                    ).show()
                }
            }
        }.start()
    }

    private fun restoreSavedCreditConsentState() {
        if (!BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) return
        val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)?.trim().orEmpty()
        if (leadCorrelationId.isEmpty()) return
        val saved = WorkflowState.getEmailConsentIdForLead(this, leadCorrelationId)?.trim().orEmpty()
        if (saved.isEmpty()) return
        creditConsentId = saved
        CommandApiService.getConsentStatus(
            context = this,
            consentId = saved,
            onSuccess = { resp ->
                val sid = resp.consentId.ifBlank { saved }
                creditConsentId = sid
                creditConsentStatus = resp.status
                WorkflowState.setEmailConsentIdForLead(this, leadCorrelationId, sid)
                runOnUiThread {
                    renderCreditConsentStatus(resp.status)
                }
                watchConsentStatus(sid)
            },
            onError = {
                // Keep saved id; network blips should not wipe state. Poll until GET succeeds.
                runOnUiThread { renderCreditConsentStatus("pending") }
                watchConsentStatus(saved)
            }
        )
    }

    private fun watchConsentStatus(consentId: String) {
        synchronized(consentPollLock) {
            if (consentPollRunning) return
            consentPollRunning = true
        }
        Thread {
            try {
                var round = 0
                while (round < CONSENT_POLL_MAX_ROUNDS) {
                    try {
                        Thread.sleep(1500)
                        if (tryUpdateConsentStatus(consentId)) break
                    } catch (_: Exception) {
                        // keep polling
                    }
                    round++
                }
            } finally {
                synchronized(consentPollLock) {
                    consentPollRunning = false
                }
            }
        }.start()
    }

    private fun tryUpdateConsentStatus(consentId: String): Boolean {
        var final = false
        val latch = java.util.concurrent.CountDownLatch(1)
        CommandApiService.getConsentStatus(
            context = this,
            consentId = consentId,
            onSuccess = { resp ->
                val status = resp.status.lowercase(Locale.getDefault())
                creditConsentId = resp.consentId.ifBlank { consentId }
                creditConsentStatus = status
                val delivery = resp.raw.optJSONObject("delivery")
                val dispatch =
                    delivery?.optString("emailDispatch")?.lowercase(Locale.getDefault()).orEmpty()
                runOnUiThread {
                    renderCreditConsentStatus(status)
                    if (dispatch == "failed" && consentEmailFailureToastShownFor != consentId) {
                        consentEmailFailureToastShownFor = consentId
                        Toast.makeText(
                            this,
                            getString(
                                R.string.credit_consent_email_failed,
                                delivery?.optString("warning").orEmpty().ifBlank { "unknown" }
                            ),
                            Toast.LENGTH_LONG
                        ).show()
                    }
                    if (dispatch == "sent" && consentEmailSentToastShownFor != consentId) {
                        consentEmailSentToastShownFor = consentId
                        Toast.makeText(
                            this,
                            getString(R.string.credit_consent_email_delivered_hint),
                            Toast.LENGTH_SHORT
                        ).show()
                    }
                }
                final = status == "approved" || status == "rejected" || status == "expired" || status == "revoked"
                latch.countDown()
            },
            onError = {
                val normalized = it.lowercase(Locale.getDefault())
                if (normalized.contains("consent_not_found") || normalized.contains("http 404")) {
                    val leadId = WorkflowState.getLeadCorrelationId(this)?.trim().orEmpty()
                    creditConsentId = null
                    creditConsentStatus = "none"
                    if (leadId.isNotEmpty()) {
                        WorkflowState.clearEmailConsentIdForLead(this, leadId)
                    }
                    runOnUiThread {
                        renderCreditConsentStatus("none")
                    }
                }
                latch.countDown()
            }
        )
        latch.await(3500, java.util.concurrent.TimeUnit.MILLISECONDS)
        return final
    }

    private fun renderCreditConsentStatus(statusRaw: String) {
        if (!BuildConfig.ENABLE_EMAIL_CONSENT_FLOW) return
        val status = statusRaw.lowercase(Locale.getDefault())
        val statusView = findViewById<TextView>(R.id.tvCreditConsentStatus)
        when (status) {
            "approved" -> {
                statusView.text = getString(R.string.credit_consent_status_approved)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.credit_band_good))
            }
            "pending" -> {
                statusView.text = getString(R.string.credit_consent_status_pending)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
            }
            "rejected" -> {
                statusView.text = getString(R.string.credit_consent_status_rejected)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.credit_band_unfavourable))
            }
            "expired" -> {
                statusView.text = getString(R.string.credit_consent_status_expired)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.credit_band_unfavourable))
            }
            "revoked" -> {
                statusView.text = getString(R.string.credit_consent_status_revoked)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.credit_band_unfavourable))
            }
            else -> {
                statusView.text = getString(R.string.credit_consent_status_none)
                statusView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
            }
        }
        val runButton = findViewById<MaterialButton>(R.id.btnQuickCreditCheck)
        runButton.isEnabled = status == "approved"
    }

    private fun resolveCreditBand(score: Int): CreditBand {
        val normalized = score.coerceIn(0, 999)
        return when (normalized) {
            in 767..999 -> CreditBand("EXCELLENT", "767 - 999", R.color.credit_band_excellent)
            in 681..766 -> CreditBand("GOOD", "681 - 766", R.color.credit_band_good)
            in 614..680 -> CreditBand("FAVOURABLE", "614 - 680", R.color.credit_band_favourable)
            in 583..613 -> CreditBand("AVERAGE", "583 - 613", R.color.credit_band_average)
            in 527..582 -> CreditBand("BELOW AVERAGE", "527 - 582", R.color.credit_band_below_average)
            in 487..526 -> CreditBand("UNFAVOURABLE", "487 - 526", R.color.credit_band_unfavourable)
            else -> CreditBand("POOR", "0 - 486", R.color.credit_band_poor)
        }
    }

    private fun renderCreditScoreUi(score: Int?, band: CreditBand?) {
        val ring = findViewById<CircularProgressIndicator>(R.id.creditScoreRing)
        val ringTrack = findViewById<CircularProgressIndicator>(R.id.creditScoreRingTrack)
        val valueView = findViewById<TextView>(R.id.tvCreditScoreValue)
        val bandView = findViewById<TextView>(R.id.tvCreditScoreBand)

        ring.max = 999
        ringTrack.max = 999
        ringTrack.setProgressCompat(999, false)

        if (score == null || band == null) {
            ring.setProgressCompat(0, false)
            ring.setIndicatorColor(ContextCompat.getColor(this, R.color.divider))
            valueView.text = "-"
            valueView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
            bandView.text = getString(R.string.credit_score_unknown)
            bandView.setTextColor(ContextCompat.getColor(this, R.color.text_secondary))
            return
        }

        val normalized = score.coerceIn(0, 999)
        ring.setProgressCompat(normalized, true)
        val color = ContextCompat.getColor(this, band.colorResId)
        ring.setIndicatorColor(color)
        valueView.text = normalized.toString()
        valueView.setTextColor(color)
        bandView.text = band.label
        bandView.setTextColor(color)
    }

    private fun queueCommunicationLog(note: String, channel: String, leadCorrelationId: String) {
        val nowIso = java.time.Instant.now().toString()
        val payload = JSONObject().apply {
            put("leadCorrelationId", leadCorrelationId)
            put("channel", channel)
            put("content", note)
            put("entityClass", "Modules\\Leads\\Entities\\Lead")
            put(
                "comment",
                JSONObject().apply {
                    put("id", "c_${System.currentTimeMillis()}")
                    put("parent", "")
                    put("created", nowIso)
                    put("modified", nowIso)
                    put("content", note)
                    put("fullname", AuthStore.getDisplayName(this@LicenseResultActivity).orEmpty().ifBlank { "You" })
                    put("profile_picture_url", "")
                    put("created_by_current_user", true)
                    put("upvote_count", 0)
                    put("user_has_upvoted", false)
                }
            )
        }
        CommandApiService.createCommand(
            context = this,
            commandType = "LOG_COMMUNICATION",
            payload = payload,
            onSuccess = { resp ->
                watchCommunicationCompletion(resp.correlationId)
            },
            onError = { err ->
                runOnUiThread {
                    Toast.makeText(this, "Communication log failed: $err", Toast.LENGTH_LONG).show()
                }
            }
        )
    }

    private fun watchCommunicationCompletion(correlationId: String) {
        Thread {
            repeat(8) {
                try {
                    Thread.sleep(1200)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    when (status.status.lowercase(Locale.getDefault())) {
                        "done" -> {
                            val comm = status.result?.optJSONObject("communication")
                            val channel = comm?.optString("channel").orEmpty().ifBlank { "note" }
                            val content = comm?.optString("content").orEmpty()
                            val loggedAt = comm?.optString("postedAt").orEmpty()
                            val line = "Last: [$channel] $content${if (loggedAt.isNotBlank()) " (${loggedAt.take(19).replace('T', ' ')})" else ""}"
                            saveLastCommunication(line)
                            runOnUiThread {
                                renderLastCommunication()
                                Toast.makeText(this, "Communication logged to CRM timeline", Toast.LENGTH_LONG).show()
                                findViewById<EditText>(R.id.etCommunicationNote).setText("")
                            }
                            return@Thread
                        }
                        "failed" -> {
                            runOnUiThread {
                                Toast.makeText(this, "Communication log failed: ${status.error ?: "Unknown error"}", Toast.LENGTH_LONG).show()
                            }
                            return@Thread
                        }
                    }
                } catch (_: Exception) {
                }
            }
        }.start()
    }

    private fun saveLastCommunication(text: String) {
        getSharedPreferences(COMM_PREFS, MODE_PRIVATE)
            .edit()
            .putString(COMM_LAST_TEXT, text)
            .apply()
    }

    private fun renderLastCommunication() {
        val value = getSharedPreferences(COMM_PREFS, MODE_PRIVATE)
            .getString(COMM_LAST_TEXT, "")
            .orEmpty()
        findViewById<TextView>(R.id.tvLastCommunication).text =
            value.ifBlank { getString(R.string.communication_last_placeholder) }
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
