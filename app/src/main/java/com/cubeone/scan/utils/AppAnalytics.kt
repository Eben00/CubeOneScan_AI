package com.cubeone.scan.utils

import android.content.Context
import android.os.Bundle
import com.cubeone.scan.BuildConfig
import com.cubeone.scan.core.auth.AuthStore
import com.google.firebase.analytics.ktx.analytics
import com.google.firebase.ktx.Firebase

object AppAnalytics {
    private fun baseBundle(context: Context): Bundle = Bundle().apply {
        putString("dealer_id", AuthStore.getDealerId(context).orEmpty())
        putString("user_id", AuthStore.getUserId(context).orEmpty())
        putString("brand", BuildConfig.FLAVOR)
    }

    fun logAppOpen(context: Context) {
        Firebase.analytics.logEvent("app_open", baseBundle(context))
    }

    fun logConsentSendTapped(context: Context, leadCorrelationId: String) {
        val params = baseBundle(context).apply {
            putString("lead_correlation_id", leadCorrelationId)
        }
        Firebase.analytics.logEvent("consent_send_tapped", params)
    }

    fun logConsentStatusApproved(context: Context, leadCorrelationId: String, consentId: String) {
        val params = baseBundle(context).apply {
            putString("lead_correlation_id", leadCorrelationId)
            putString("consent_id", consentId)
        }
        Firebase.analytics.logEvent("consent_status_approved", params)
    }

    fun logCreditScoreRendered(context: Context, leadCorrelationId: String, score: Int, band: String) {
        val params = baseBundle(context).apply {
            putString("lead_correlation_id", leadCorrelationId)
            putInt("score", score)
            putString("credit_band", band)
        }
        Firebase.analytics.logEvent("credit_score_rendered", params)
    }
}
