package com.cubeone.scan.core.auth

import android.content.Context

object AuthStore {
    private const val PREFS_NAME = "cubeone_auth_prefs"
    private const val KEY_ACCESS_TOKEN = "access_token"
    private const val KEY_REFRESH_TOKEN = "refresh_token"
    private const val KEY_USER_ID = "user_id"
    private const val KEY_DEALER_ID = "dealer_id"
    private const val KEY_BRANCH_ID = "branch_id"
    private const val KEY_ROLE = "role"
    private const val KEY_USER_EMAIL = "user_email"
    private const val KEY_DISPLAY_NAME = "display_name"
    private const val KEY_DEALERSHIP_NAME = "dealership_name"
    private const val KEY_MUST_CHANGE_PASSWORD = "must_change_password"

    private fun prefs(context: Context) =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    fun saveTokens(
        context: Context,
        accessToken: String,
        refreshToken: String?,
        userId: String? = null,
        dealerId: String? = null,
        branchId: String? = null,
        role: String? = null,
        userEmail: String? = null,
        displayName: String? = null,
        dealershipName: String? = null,
        mustChangePassword: Boolean? = null
    ) {
        val editor = prefs(context).edit()
            .putString(KEY_ACCESS_TOKEN, accessToken)
        if (refreshToken != null) {
            editor.putString(KEY_REFRESH_TOKEN, refreshToken)
        } else {
            editor.remove(KEY_REFRESH_TOKEN)
        }
        if (!userId.isNullOrBlank()) editor.putString(KEY_USER_ID, userId) else editor.remove(KEY_USER_ID)
        if (!dealerId.isNullOrBlank()) editor.putString(KEY_DEALER_ID, dealerId) else editor.remove(KEY_DEALER_ID)
        if (!branchId.isNullOrBlank()) editor.putString(KEY_BRANCH_ID, branchId) else editor.remove(KEY_BRANCH_ID)
        if (!role.isNullOrBlank()) editor.putString(KEY_ROLE, role) else editor.remove(KEY_ROLE)
        if (!userEmail.isNullOrBlank()) editor.putString(KEY_USER_EMAIL, userEmail) else editor.remove(KEY_USER_EMAIL)
        if (!displayName.isNullOrBlank()) editor.putString(KEY_DISPLAY_NAME, displayName) else editor.remove(KEY_DISPLAY_NAME)
        if (!dealershipName.isNullOrBlank()) editor.putString(KEY_DEALERSHIP_NAME, dealershipName) else editor.remove(KEY_DEALERSHIP_NAME)
        if (mustChangePassword != null) editor.putBoolean(KEY_MUST_CHANGE_PASSWORD, mustChangePassword)
        editor.apply()
    }

    fun getAccessToken(context: Context): String? =
        prefs(context).getString(KEY_ACCESS_TOKEN, null)

    fun getDealerId(context: Context): String? =
        prefs(context).getString(KEY_DEALER_ID, null)

    fun getUserId(context: Context): String? =
        prefs(context).getString(KEY_USER_ID, null)

    fun getBranchId(context: Context): String? =
        prefs(context).getString(KEY_BRANCH_ID, null)

    fun getRole(context: Context): String? =
        prefs(context).getString(KEY_ROLE, null)

    fun getUserEmail(context: Context): String? =
        prefs(context).getString(KEY_USER_EMAIL, null)

    fun getDisplayName(context: Context): String? =
        prefs(context).getString(KEY_DISPLAY_NAME, null)

    fun getDealershipName(context: Context): String? =
        prefs(context).getString(KEY_DEALERSHIP_NAME, null)

    fun isMustChangePassword(context: Context): Boolean =
        prefs(context).getBoolean(KEY_MUST_CHANGE_PASSWORD, false)

    fun clear(context: Context) {
        prefs(context).edit().clear().apply()
    }
}

