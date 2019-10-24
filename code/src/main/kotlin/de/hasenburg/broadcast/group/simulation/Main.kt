package de.hasenburg.broadcast.group.simulation

import de.hasenburg.broadcast.group.simulation.model.BrokerId
import de.hasenburg.broadcast.group.simulation.model.generateRandomBrokerLcms
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

    if (args.isEmpty()) {
        runRandomSimulation()
    } else {
        runDefinedSimulation(args.toList())
    }

}

fun runRandomSimulation() {
    val brokerLocations = generateRandomBrokerLocations(5000)
    val brokerLcms: Map<BrokerId, Int> = brokerLocations.generateRandomBrokerLcms(1000)
    val latencyThresholds = listOf(10.0, 20.0, 50.0, 100.0)

    val runtime = measureTimeMillis {
        logger.info("Simulation with ${brokerLocations.size} brokers.")
        runBlocking {
            latencyThresholds.forEach {
                logger.info("Latency threshold = $it, number of leaders = ${runSimulation(it,
                        brokerLocations,
                        brokerLcms)}")
                repeat(3) { logger.info("") }
            }
        }
    }
    logger.info("Simulation took $runtime ms")
}

fun runDefinedSimulation(args: List<String>) {
    TODO()
}
