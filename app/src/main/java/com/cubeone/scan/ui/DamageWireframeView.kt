package com.cubeone.scan.ui

import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.RectF
import android.util.AttributeSet
import android.view.MotionEvent
import android.view.View
import kotlin.math.min

data class DamageMarker(
    val xRatio: Float,
    val yRatio: Float,
    val zone: String
)

class DamageWireframeView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null
) : View(context, attrs) {

    private val borderPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.parseColor("#102A43")
        style = Paint.Style.STROKE
        strokeWidth = 8f
    }
    private val bodyFillPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.parseColor("#D9E2EC")
        style = Paint.Style.FILL
    }
    private val cabinFillPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.parseColor("#BCCCDC")
        style = Paint.Style.FILL
    }
    private val markerPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.parseColor("#FF7043")
        style = Paint.Style.FILL
    }
    private val textPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.parseColor("#102A43")
        textSize = 30f
        isFakeBoldText = true
    }

    private val markers = mutableListOf<DamageMarker>()
    private var onMarkerTapped: ((DamageMarker) -> Unit)? = null

    fun setOnMarkerTappedListener(listener: (DamageMarker) -> Unit) {
        onMarkerTapped = listener
    }

    fun addMarker(xRatio: Float, yRatio: Float, zone: String) {
        markers.add(
            DamageMarker(
                xRatio = xRatio.coerceIn(0f, 1f),
                yRatio = yRatio.coerceIn(0f, 1f),
                zone = zone
            )
        )
        invalidate()
    }

    fun clearMarkers() {
        markers.clear()
        invalidate()
    }

    fun getMarkers(): List<DamageMarker> = markers.toList()

    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)
        val w = width.toFloat()
        val h = height.toFloat()
        if (w <= 0f || h <= 0f) return

        // Top-view car wireframe.
        val bodyLeft = w * 0.24f
        val bodyRight = w * 0.76f
        val bodyTop = h * 0.07f
        val bodyBottom = h * 0.93f
        val body = RectF(bodyLeft, bodyTop, bodyRight, bodyBottom)
        canvas.drawRoundRect(body, 44f, 44f, bodyFillPaint)
        canvas.drawRoundRect(body, 44f, 44f, borderPaint)

        // Hood / windscreen / roof / rear glass / boot separators.
        val hoodY = h * 0.20f
        val windscreenY = h * 0.31f
        val rearGlassY = h * 0.69f
        val bootY = h * 0.80f
        canvas.drawLine(bodyLeft + 6f, hoodY, bodyRight - 6f, hoodY, borderPaint)
        canvas.drawLine(bodyLeft + 10f, windscreenY, bodyRight - 10f, windscreenY, borderPaint)
        canvas.drawLine(bodyLeft + 10f, rearGlassY, bodyRight - 10f, rearGlassY, borderPaint)
        canvas.drawLine(bodyLeft + 6f, bootY, bodyRight - 6f, bootY, borderPaint)

        // Center spine.
        canvas.drawLine(w * 0.5f, bodyTop + 10f, w * 0.5f, bodyBottom - 10f, borderPaint)

        // Cabin contour.
        val cabin = RectF(w * 0.33f, h * 0.27f, w * 0.67f, h * 0.73f)
        canvas.drawRoundRect(cabin, 28f, 28f, cabinFillPaint)
        canvas.drawRoundRect(cabin, 28f, 28f, borderPaint)

        // Wheels.
        val wheelW = w * 0.10f
        val wheelH = h * 0.16f
        val wheelInset = w * 0.03f
        val frontWheelTop = h * 0.20f
        val rearWheelTop = h * 0.64f
        canvas.drawRoundRect(
            RectF(bodyLeft - wheelInset - wheelW, frontWheelTop, bodyLeft - wheelInset, frontWheelTop + wheelH),
            16f,
            16f,
            borderPaint
        )
        canvas.drawRoundRect(
            RectF(bodyRight + wheelInset, frontWheelTop, bodyRight + wheelInset + wheelW, frontWheelTop + wheelH),
            16f,
            16f,
            borderPaint
        )
        canvas.drawRoundRect(
            RectF(bodyLeft - wheelInset - wheelW, rearWheelTop, bodyLeft - wheelInset, rearWheelTop + wheelH),
            16f,
            16f,
            borderPaint
        )
        canvas.drawRoundRect(
            RectF(bodyRight + wheelInset, rearWheelTop, bodyRight + wheelInset + wheelW, rearWheelTop + wheelH),
            16f,
            16f,
            borderPaint
        )

        canvas.drawText("FRONT", w * 0.39f, h * 0.05f, textPaint)
        canvas.drawText("BACK", w * 0.41f, h * 0.985f, textPaint)
        canvas.drawText("L", w * 0.08f, h * 0.52f, textPaint)
        canvas.drawText("R", w * 0.90f, h * 0.52f, textPaint)

        val r = min(w, h) * 0.02f
        markers.forEachIndexed { index, marker ->
            val cx = marker.xRatio * w
            val cy = marker.yRatio * h
            canvas.drawCircle(cx, cy, r * 1.5f, markerPaint)
            canvas.drawText((index + 1).toString(), cx + r * 1.8f, cy - r * 1.2f, textPaint)
        }
    }

    override fun onTouchEvent(event: MotionEvent): Boolean {
        if (event.action == MotionEvent.ACTION_UP) {
            val x = (event.x / width.toFloat()).coerceIn(0f, 1f)
            val y = (event.y / height.toFloat()).coerceIn(0f, 1f)
            val zone = classifyZone(x, y)
            val marker = DamageMarker(x, y, zone)
            onMarkerTapped?.invoke(marker)
            return true
        }
        return true
    }

    private fun classifyZone(x: Float, y: Float): String {
        return when {
            y < 0.22f -> "front"
            y > 0.78f -> "back"
            x < 0.35f -> "left_side"
            x > 0.65f -> "right_side"
            else -> "roof_center"
        }
    }
}
