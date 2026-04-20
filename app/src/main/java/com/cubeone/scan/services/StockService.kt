package com.cubeone.scan.services

import com.cubeone.scan.models.StockUnit
import org.json.JSONObject

object StockService {

    fun createStockUnit(stock: StockUnit) {

        val data = JSONObject()

        data.put("vin", stock.vin)
        data.put("price", stock.price)
        data.put("location", stock.location)

        ApiService.sendEvent("create_stock_unit", data)

    }

}