package de.hasenburg.broadcast.group.simulation

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import de.hasenburg.broadcast.group.simulation.model.*
import kotlinx.coroutines.runBlocking
import org.apache.logging.log4j.LogManager
import java.io.File
import kotlin.system.measureTimeMillis

private val logger = LogManager.getLogger()

const val msPerKm = 0.021048134571484346 // derived from 2016 IPlane data

fun main(args: Array<String>) {

    if (args.isEmpty()) {
        logger.info("No args provided, running default simulation")
        runRandomSimulation()
    } else {
        logger.info("Running world cities simulation")
        runWorldCitiesSimulation(args)
    }

}

fun runRandomSimulation() {
    val brokerLocations = generateRandomBrokerLocations(1000)
    val brokerLcms: Map<BrokerId, Int> = brokerLocations.generateRandomBrokerLcms(1000)
    val latencyThresholds = listOf(10.0, 50.0, 100.0, 150.0, 200.0)

    val runtime = measureTimeMillis {
        logger.info("Simulation with ${brokerLocations.size} brokers.")
        runBlocking {
            latencyThresholds.forEach {
                val simulationResult = runSimulation(it, brokerLocations, brokerLcms)
                logger.info("Latency threshold = $it, number of leaders = ${simulationResult.brokers.numberOfLeaders()}")
                simulationResult.saveExperimentData(filePrefix = "randomResult-$it-${brokerLocations.size}")
            }
        }
    }
    logger.info("Running all simulations took $runtime ms")
}

/**
 * For now, the only defined simulation is based on the world cities data from https://simplemaps.com/data/world-cities.
 */
fun runWorldCitiesSimulation(args: Array<String>) {
    val conf = mainBody { ArgParser(args).parseInto(::Conf) }
    val (brokerLocations, brokerLcms) = getLocationsAndLcms(conf.inputFile, conf.maxBrokers)

    val runtime = measureTimeMillis {
        logger.info("Simulation with ${brokerLocations.size} brokers.")
        runBlocking {
            conf.latencyThresholds.forEach {
                val simulationResult = runSimulation(it, brokerLocations, brokerLcms)
                logger.info("Latency threshold = $it, number of leaders = ${simulationResult.brokers.numberOfLeaders()}")
                simulationResult.saveExperimentData(filePrefix = "${conf.simulationPrefix}-$it-${brokerLocations.size}")
            }
        }
    }
    logger.info("Running all simulations took $runtime ms")

}

/* ************************************************
   ******* WorldCities ****************************
   ************************************************ */

private fun getLocationsAndLcms(ipFile: File, maxBrokers: Int): Pair<Map<BrokerId, Location>, Map<BrokerId, Int>> {
    var brokerData = ipFile.readLines().drop(1).map { it.getBrokerData() }

    if (brokerData.size > maxBrokers) {
        logger.info("Found ${brokerData.size} brokers in data set, but max broker is set to $maxBrokers, ignoring additionals")
        brokerData = brokerData.subList(0, maxBrokers)
    }

    // preserve only unique unique locations
    brokerData = brokerData.groupBy { Location(it.lat, it.lon) }.map {
        if (it.value.size > 1) {
            logger.warn("${it.value.size} brokers are at ${it.key}, picking ${it.value[0]}")
        }
        it.value[0]
    }

    val brokerLocations = brokerData.map { BrokerId(it.name) to Location(it.lat, it.lon) }.toMap()
    val brokerLcms = brokerData.map { BrokerId(it.name) to it.lcm }.toMap()

    return Pair(brokerLocations, brokerLcms)
}

private data class BrokerData(val name: String, val lat: Double, val lon: Double, val country: String, val lcm: Int)

/**
 * Might throw a [NumberFormatException].
 */
private fun String.getBrokerData(): BrokerData {
    val split = this.split("\",\"")
    check(split.size == 11) { "$this is not a valid world city row" }
    val lcm = split[9].toIntOrNull() ?: 1
    val id = split[10].replace("\"", "")

    return BrokerData("${split[1]}-$id", split[2].toDouble(), split[3].toDouble(), split[4], lcm)
}

class Conf(parser: ArgParser) {
    val inputFile by parser
        .storing("-i", "--input", help = "local directory containing the input file") { File(this) }
        .default(File("data/simulation_input/worldcities.csv"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("${value.absolutePath} does not exist")
            }
            if (!value.isFile) {
                throw InvalidArgumentException("${value.absolutePath} is not a file")
            }
        }

    val simulationPrefix by parser
        .storing("-p", "--prefix", help = "simulation prefix used to name output files")

    val latencyThresholds by parser
        .storing("-l", "--latencyTresholds", help = "latency thresholds, e.g., 10.0,20.0,30.0") {
            this.split(",")
                .map { it.toDouble() }
        }
        .default(listOf(10.0, 20.0, 30.0))

    val maxBrokers by parser
        .storing("-m",
                "--max",
                help = "the maximum number of brokers that should be part of the simulation") { this.toInt() }
        .default { Int.MAX_VALUE }

    init {
        logger.info("Configuration: inputFile = ${inputFile.absoluteFile}, simulationPrefix = $simulationPrefix, latencyThresholds = $latencyThresholds")
    }
}
