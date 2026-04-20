package com.cubeone.scan.services

import android.util.Log
import com.cubeone.scan.models.Customer

object ContactService {

    fun createCustomer(customer: Customer) {

        Log.i("CubeOne", "Customer created: ${customer.firstName} ${customer.surname}")

        // Later this will connect to:
        // CubeOne CRM
        // Evolve system
        // Dealer database

    }

}