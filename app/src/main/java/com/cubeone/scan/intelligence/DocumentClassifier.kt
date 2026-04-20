package com.cubeone.scan.intelligence

import android.graphics.Bitmap

object DocumentClassifier {

    fun classify(bitmap: Bitmap): String {

        val ratio = bitmap.width.toFloat() / bitmap.height.toFloat()

        return when {

            ratio > 1.5 -> "DRIVER_LICENSE"

            ratio < 1.2 -> "LICENSE_DISK"

            else -> "UNKNOWN"
        }
    }
}