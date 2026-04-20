package com.cubeone.scan.services

import com.cubeone.scan.models.TradeIn
import org.json.JSONObject

object TradeInService {

    fun createTradeIn(tradeIn: TradeIn) {

        val data = JSONObject()

        data.put("vin", tradeIn.vin)
        data.put("mileage", tradeIn.mileage)
        data.put("condition", tradeIn.condition)

        ApiService.sendEvent("create_tradein", data)

    }

}