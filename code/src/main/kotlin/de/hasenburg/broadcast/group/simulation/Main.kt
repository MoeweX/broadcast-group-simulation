package de.hasenburg.broadcast.group.simulation

import de.hasenburg.broadcast.group.simulation.model.generateRandomBrokerLocations
import de.hasenburg.broadcast.group.simulation.model.runSimulation
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import kotlin.concurrent.timerTask
import kotlin.system.measureTimeMillis

private val logger = LogManager.getLogger()

const val msPerKm = 0.1

fun main(args: Array<String>) {
    val runtime = measureTimeMillis {
        runBlocking {
            runSimulation(20.0, generateRandomBrokerLocations(1000))
        }
    }
    logger.info("Simulation took $runtime ms")
}
