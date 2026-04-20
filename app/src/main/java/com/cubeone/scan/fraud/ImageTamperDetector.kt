package com.cubeone.scan.fraud

import android.graphics.Bitmap

object ImageTamperDetector {

    fun detect(bitmap: Bitmap): Boolean {

        val width = bitmap.width
        val height = bitmap.height

        var suspiciousPixels = 0

        for (x in 0 until width step 20) {
            for (y in 0 until height step 20) {

                val pixel = bitmap.getPixel(x, y)

                val r = pixel shr 16 and 0xff
                val g = pixel shr 8 and 0xff
                val b = pixel and 0xff

                if (r == g && g == b) {
                    suspiciousPixels++
                }
            }
        }

        return suspiciousPixels > 200
    }
}