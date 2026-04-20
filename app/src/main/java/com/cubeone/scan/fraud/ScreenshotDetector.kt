package com.cubeone.scan.fraud

import android.graphics.Bitmap

object ScreenshotDetector {

    fun detect(bitmap: Bitmap): Boolean {

        val width = bitmap.width
        val height = bitmap.height

        // screenshots often have perfect resolution
        if (width == 1080 && height == 1920) return true

        if (width == 1440 && height == 2560) return true

        return false
    }
}