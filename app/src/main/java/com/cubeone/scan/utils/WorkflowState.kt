package com.cubeone.scan.utils

import android.content.Context
import android.content.SharedPreferences

object WorkflowState {
    private const val PREFS_NAME = "cubeone_workflow_state"
    private const val KEY_LEAD_CORRELATION_ID = "lead_correlation_id"
    private const val KEY_STOCK_CORRELATION_ID = "stock_correlation_id"

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
}

