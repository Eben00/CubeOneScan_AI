package com.cubeone.scan.services

import android.util.Log
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL

object ApiService {

    private const val API_URL = "https://api.cubeone.ai/event"

    fun sendEvent(event: String, data: JSONObject) {

        Thread {

            try {

                val url = URL(API_URL)

                val conn = url.openConnection() as HttpURLConnection

                conn.requestMethod = "POST"
                conn.setRequestProperty("Content-Type", "application/json")
                conn.doOutput = true

                val payload = JSONObject()

                payload.put("event", event)
                payload.put("data", data)

                conn.outputStream.write(payload.toString().toByteArray())

                val response = conn.responseCode

                Log.i("CubeOneAPI", "Response: $response")

            } catch (e: Exception) {

                Log.e("CubeOneAPI", "API error", e)

            }

        }.start()

    }
}