package com.cubeone.scan.core.api

import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import java.util.concurrent.TimeUnit

class ApiClient {

    data class HttpResult(val statusCode: Int, val body: String?)

    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .build()

    private val jsonMediaType = "application/json; charset=utf-8".toMediaType()

    fun post(url: String, jsonBody: String): String? {
        return try {
            val result = postDetailed(url, jsonBody)
            if (result.statusCode in 200..299) result.body else null
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    fun postDetailed(url: String, jsonBody: String): HttpResult {
        return try {
            val body = jsonBody.toRequestBody(jsonMediaType)
            val request = Request.Builder()
                .url(url)
                .post(body)
                .build()

            client.newCall(request).execute().use { response ->
                HttpResult(response.code, response.body?.string())
            }
        } catch (e: Exception) {
            e.printStackTrace()
            HttpResult(-1, null)
        }
    }

    fun get(url: String): String? {
        return try {
            val request = Request.Builder()
                .url(url)
                .get()
                .build()

            client.newCall(request).execute().use { response ->
                if (response.isSuccessful) {
                    response.body?.string()
                } else {
                    null
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
}