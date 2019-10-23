package de.hasenburg.broadcast.group.simulation.model

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import kotlin.random.Random

private val logger = LogManager.getLogger()

suspend fun runSimulation(latencyThreshold: Double,
                          brokerLocations: Map<BrokerId, Location> = generateRandomBrokerLocations()) =
        coroutineScope {
            val brokerChannels = generateBrokerChannel(brokerLocations.keys)
            val brokerJobs = mutableListOf<Deferred<Broker>>()

            for ((brokerId, location) in brokerLocations) {
                brokerJobs.add(async {
                    Broker(brokerId,
                            Random.nextInt(1, 1000),
                            location,
                            brokerChannels,
                            latencyThreshold,
                            brokerLocations)
                })
            }

            val brokers = brokerJobs.awaitAll()
            var oldLeaders: Int
            var tick = 0
            logger.info("All brokers ready")

            do {
                oldLeaders = brokers.numberOfLeaders()
                logger.info("Starting tick ${++tick}")
                coroutineScope {
                    brokers.forEach {launch { it.startNewTick() }}
                }

                logger.debug("Sending MergeRequests")
                val leaders = brokers.leaders()
                coroutineScope {
                    brokers.forEach {launch { it.sendMergeRequest(leaders) }}
                }

                TODO("Other phases")

            } while (oldLeaders != brokers.numberOfLeaders())
        }


private fun List<Broker>.leaders() : List<BrokerId> {
    return this.filter {it.isLeader}.map { it.brokerId }
}

private fun List<Broker>.numberOfLeaders(): Int {
    return this.filter { it.isLeader }.size
}

private fun generateRandomBrokerLocations(): Map<BrokerId, Location> {
    val tmpMap = mutableMapOf<BrokerId, Location>()
    val bounding = Geofence.circle(Location(30.0, 30.0), 5.0)

    repeat(10) {
        tmpMap[BrokerId("Broker-$it")] = Location.randomInGeofence(bounding)
    }

    return tmpMap
}

private fun generateBrokerChannel(brokerIds: Set<BrokerId>): Map<BrokerId, Channel<BrokerMessage>> {
    val tmpMap = mutableMapOf<BrokerId, Channel<BrokerMessage>>()

    for (brokerId in brokerIds) {
        tmpMap[brokerId] = Channel(Channel.UNLIMITED)
    }

    return tmpMap
}
