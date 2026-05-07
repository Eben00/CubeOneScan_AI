package com.cubeone.scan.ui

import android.database.Cursor
import android.net.Uri
import android.os.Bundle
import android.provider.OpenableColumns
import android.util.Base64
import android.view.LayoutInflater
import android.widget.LinearLayout
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.services.CommandApiService
import com.google.android.material.button.MaterialButton
import com.google.android.material.textfield.TextInputEditText
import org.json.JSONObject
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class DealDocumentsActivity : AppCompatActivity() {
    private data class DealDocument(val id: String, val label: String)
    private companion object {
        private const val PREFS_DEAL_DOCS = "deal_docs"
        private const val KEY_DEAL_REF = "deal_ref"
        private const val KEY_CUSTOMER_REF = "customer_ref"
    }

    private val documents = listOf(
        DealDocument("completed_otp", "Completed OTP"),
        DealDocument("signed_tc", "Signed T&C"),
        DealDocument("bank_podium", "Bank Podium"),
        DealDocument("sa_id_document", "SA ID Document"),
        DealDocument("drivers_license", "Drivers License"),
        DealDocument("payslips", "Payslips"),
        DealDocument("proof_of_address_applicant", "Proof of Address (applicant)"),
        DealDocument("invoice", "Invoice"),
        DealDocument("signed_finance_contract", "Signed Finance Contract"),
        DealDocument("proof_of_insurance_cover", "Proof of Insurance Cover"),
        DealDocument("signed_credit_life", "Signed Credit Life"),
        DealDocument("signed_topup_insurance", "Signed Top-up Insurance"),
        DealDocument("signed_mechanical_warranty", "Signed Mechanical Warranty"),
        DealDocument("natis_title_holder_license", "NATIS Title Holder / License"),
        DealDocument("signed_bank_delivery_note", "Signed Bank Delivery Note"),
        DealDocument("bank_cash_payment_advice", "Bank / Cash Payment Advice"),
        DealDocument("dealer_invoice", "Dealer Invoice"),
        DealDocument("fsp_28260_disclosure_doc", "FSP 28260 Disclosure Doc (incl. accrual parties)"),
        DealDocument("needs_analysis_roa", "Needs Analysis and ROA"),
        DealDocument("declarations", "Declarations")
    )

    private var pendingDocument: DealDocument? = null
    private val uploadButtons = mutableMapOf<String, MaterialButton>()
    private val statusViews = mutableMapOf<String, TextView>()
    private lateinit var etDealRef: TextInputEditText
    private lateinit var etCustomerRef: TextInputEditText

    private val pickDocumentLauncher =
        registerForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
            val document = pendingDocument ?: return@registerForActivityResult
            if (uri == null) return@registerForActivityResult
            uploadDocument(document, uri)
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_deal_documents)
        etDealRef = findViewById(R.id.etDealRef)
        etCustomerRef = findViewById(R.id.etCustomerRef)
        etDealRef.setText(getSharedPreferences(PREFS_DEAL_DOCS, MODE_PRIVATE).getString(KEY_DEAL_REF, "").orEmpty())
        etCustomerRef.setText(getSharedPreferences(PREFS_DEAL_DOCS, MODE_PRIVATE).getString(KEY_CUSTOMER_REF, "").orEmpty())

        val container = findViewById<LinearLayout>(R.id.layoutDealDocuments)
        val inflater = LayoutInflater.from(this)
        documents.forEach { document ->
            val row = inflater.inflate(R.layout.item_deal_document, container, false)
            val name = row.findViewById<TextView>(R.id.tvDealDocName)
            val status = row.findViewById<TextView>(R.id.tvDealDocStatus)
            val upload = row.findViewById<MaterialButton>(R.id.btnUploadDealDoc)
            name.text = document.label
            status.text = getDocumentStatus(document.id)
            upload.setOnClickListener {
                val dealRef = etDealRef.text?.toString().orEmpty().trim()
                if (dealRef.isBlank()) {
                    Toast.makeText(this, getString(R.string.deal_docs_deal_ref_required), Toast.LENGTH_LONG).show()
                    return@setOnClickListener
                }
                saveReferences()
                pendingDocument = document
                pickDocumentLauncher.launch(arrayOf("image/*", "application/pdf"))
            }
            uploadButtons[document.id] = upload
            statusViews[document.id] = status
            container.addView(row)
        }

        findViewById<MaterialButton>(R.id.btnDealDocsDone).setOnClickListener {
            saveReferences()
            finish()
        }
    }

    override fun onPause() {
        super.onPause()
        saveReferences()
    }

    private fun getDocumentStatus(documentId: String): String {
        val stored = getSharedPreferences(PREFS_DEAL_DOCS, MODE_PRIVATE)
            .getString("status_$documentId", null)
        return stored ?: getString(R.string.deal_docs_status_pending)
    }

    private fun setDocumentStatus(documentId: String, status: String) {
        getSharedPreferences(PREFS_DEAL_DOCS, MODE_PRIVATE)
            .edit()
            .putString("status_$documentId", status)
            .apply()
        statusViews[documentId]?.text = status
    }

    private fun saveReferences() {
        getSharedPreferences(PREFS_DEAL_DOCS, MODE_PRIVATE)
            .edit()
            .putString(KEY_DEAL_REF, etDealRef.text?.toString().orEmpty().trim())
            .putString(KEY_CUSTOMER_REF, etCustomerRef.text?.toString().orEmpty().trim())
            .apply()
    }

    private fun uploadDocument(document: DealDocument, uri: Uri) {
        val button = uploadButtons[document.id] ?: return
        button.isEnabled = false
        setDocumentStatus(document.id, getString(R.string.deal_docs_status_uploading))

        Thread {
            try {
                val bytes = contentResolver.openInputStream(uri)?.use { it.readBytes() }
                    ?: throw IllegalStateException("Cannot read selected file")
                if (bytes.size > 5 * 1024 * 1024) {
                    throw IllegalStateException("File too large. Max 5MB per upload.")
                }
                val mime = contentResolver.getType(uri).orEmpty().ifBlank { "application/octet-stream" }
                val fileName = resolveFileName(uri).ifBlank { "${document.id}.bin" }
                val dealRef = etDealRef.text?.toString().orEmpty().trim()
                val customerRef = etCustomerRef.text?.toString().orEmpty().trim()
                val payload = JSONObject().apply {
                    put("recordType", "deal_document_upload")
                    put("documentType", document.id)
                    put("documentLabel", document.label)
                    put("dealReference", dealRef)
                    put("customerReference", customerRef)
                    put("fileName", fileName)
                    put("mimeType", mime)
                    put("sizeBytes", bytes.size)
                    put("dealerId", AuthStore.getDealerId(this@DealDocumentsActivity).orEmpty())
                    put("userId", AuthStore.getUserId(this@DealDocumentsActivity).orEmpty())
                    put("source", "mobile_deal_file")
                    put("contentBase64", Base64.encodeToString(bytes, Base64.NO_WRAP))
                }
                CommandApiService.postScanRecord(
                    context = this,
                    payload = payload,
                    onSuccess = { response ->
                        val ts = SimpleDateFormat("dd MMM HH:mm", Locale.getDefault()).format(Date())
                        val ref = response.optString("recordId")
                            .ifBlank { response.optString("correlationId") }
                            .ifBlank { "ok" }
                        runOnUiThread {
                            button.isEnabled = true
                            setDocumentStatus(
                                document.id,
                                getString(R.string.deal_docs_status_uploaded, ts, ref)
                            )
                            Toast.makeText(this, "Uploaded ${document.label}", Toast.LENGTH_SHORT).show()
                        }
                    },
                    onError = { err ->
                        // Fallback to queued command when scan endpoint is unavailable/offline.
                        CommandApiService.createCommand(
                            context = this,
                            commandType = "UPLOAD_DEAL_DOCUMENT",
                            payload = payload,
                            onSuccess = { response ->
                                val ts = SimpleDateFormat("dd MMM HH:mm", Locale.getDefault()).format(Date())
                                runOnUiThread {
                                    button.isEnabled = true
                                    setDocumentStatus(
                                        document.id,
                                        getString(R.string.deal_docs_status_uploaded, ts, response.correlationId)
                                    )
                                    Toast.makeText(this, "Uploaded ${document.label}", Toast.LENGTH_SHORT).show()
                                }
                            },
                            onError = { fallbackErr ->
                                runOnUiThread {
                                    button.isEnabled = true
                                    val queued = fallbackErr.contains("queued offline", ignoreCase = true)
                                    val status = if (queued) {
                                        getString(R.string.deal_docs_status_queued)
                                    } else {
                                        getString(R.string.deal_docs_status_failed, "$err | $fallbackErr")
                                    }
                                    setDocumentStatus(document.id, status)
                                    Toast.makeText(this, "Upload failed: $fallbackErr", Toast.LENGTH_LONG).show()
                                }
                            }
                        )
                    }
                )
            } catch (e: Exception) {
                runOnUiThread {
                    button.isEnabled = true
                    setDocumentStatus(document.id, getString(R.string.deal_docs_status_failed, e.message ?: "unknown"))
                    Toast.makeText(this, "Upload failed: ${e.message}", Toast.LENGTH_LONG).show()
                }
            }
        }.start()
    }

    private fun resolveFileName(uri: Uri): String {
        if (uri.scheme != "content") return uri.lastPathSegment.orEmpty()
        val cursor: Cursor? = contentResolver.query(uri, null, null, null, null)
        cursor?.use {
            val index = it.getColumnIndex(OpenableColumns.DISPLAY_NAME)
            if (index >= 0 && it.moveToFirst()) return it.getString(index).orEmpty()
        }
        return ""
    }
}
