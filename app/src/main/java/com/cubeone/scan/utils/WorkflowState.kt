package com.cubeone.scan.utils

import android.content.Context
import android.content.SharedPreferences

object WorkflowState {
    private const val PREFS_NAME = "cubeone_workflow_state"
    private const val KEY_LEAD_CORRELATION_ID = "lead_correlation_id"
    private const val KEY_STOCK_CORRELATION_ID = "stock_correlation_id"
    private const val KEY_EMAIL_CONSENT_PREFIX = "email_consent_id_for_lead_v1:"

    private fun prefs(context: Context): SharedPreferences =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun setLeadCorrelationId(context: Context, correlationId: String) {
        prefs(context).edit().putString(KEY_LEAD_CORRELATION_ID, correlationId).apply()
    }

    fun getLeadCorrelationId(context: Context): String? =
        prefs(context).getString(KEY_LEAD_CORRELATION_ID, null)

    fun setStockCorrelationId(context: Context, correlationId: String) {
        prefs(context).edit().putString(KEY_STOCK_CORRELATION_ID, correlationId).apply()
    }

    fun getStockCorrelationId(context: Context): String? =
        prefs(context).getString(KEY_STOCK_CORRELATION_ID, null)

    /** Remembers POST /consents id for this lead so the UI survives rotation / revisit. */
    fun setEmailConsentIdForLead(context: Context, leadCorrelationId: String, consentId: String) {
        val lead = leadCorrelationId.trim()
        val cid = consentId.trim()
        if (lead.isEmpty() || cid.isEmpty()) return
        prefs(context).edit().putString(KEY_EMAIL_CONSENT_PREFIX + lead, cid).apply()
    }

    fun getEmailConsentIdForLead(context: Context, leadCorrelationId: String): String? {
        val lead = leadCorrelationId.trim()
        if (lead.isEmpty()) return null
        return prefs(context).getString(KEY_EMAIL_CONSENT_PREFIX + lead, null)?.trim()?.ifEmpty { null }
    }

    fun clearEmailConsentIdForLead(context: Context, leadCorrelationId: String) {
        val lead = leadCorrelationId.trim()
        if (lead.isEmpty()) return
        prefs(context).edit().remove(KEY_EMAIL_CONSENT_PREFIX + lead).apply()
    }
}

