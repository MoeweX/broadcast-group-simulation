package de.hasenburg.broadcast.group.simulation.model

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import kotlin.random.Random

private val logger = LogManager.getLogger()

suspend fun runSimulation(latencyThreshold: Double,
                          brokerLocations: Map<BrokerId, Location> = generateRandomBrokerLocations(10)) =
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
                logger.debug("\n\n")
                logger.info("Starting tick ${++tick}, currently there are $oldLeaders leader")
                coroutineScope {
                    brokers.forEach { launch { it.startNewTick() } }
                }

                logger.debug("Sending MergeRequests")
                val leaders = brokers.leaders()
                coroutineScope {
                    brokers.forEach { launch { it.sendMergeRequest(leaders) } }
                }

                logger.debug("Receiving MergeRequests and Sending MergeReply")
                coroutineScope {
                    brokers.forEach { launch { it.receiveAndProcessMergeRequests() } }
                }

                logger.debug("Receiving MergeReply")
                coroutineScope {
                    brokers.forEach { launch { it.receiveMergeReply() } }
                }

                logger.debug("Notifying members about merge")
                coroutineScope {
                    brokers.forEach { launch { it.notifyMembersAboutMerge() } }
                }

                logger.debug("Leaders and Members are merging")
                coroutineScope {
                    brokers.forEach { launch { it.doMerge() } }
                }

                logger.debug("Receiving JoinInfo")
                coroutineScope {
                    brokers.forEach { launch { it.receiveJoinInfo() } }
                }

            } while (oldLeaders != brokers.numberOfLeaders())

            logger.info("Stable state reached after $tick ticks.")
            check(brokers.size == brokers.numberOfMembers() + brokers.numberOfLeaders())
            brokers.printBroadcastGroups()
        }

private fun List<Broker>.printBroadcastGroups() {
    for (broker in this) {
        if (broker.isLeader) {
            logger.info("${broker.brokerId} has these members: ${broker.membersInMyBroadcastGroup}")
        }
    }
}

private fun List<Broker>.leaders(): List<BrokerId> {
    return this.filter { it.isLeader }.map { it.brokerId }
}

private fun List<Broker>.numberOfLeaders(): Int {
    return this.filter { it.isLeader }.size
}

private fun List<Broker>.numberOfMembers(): Int {
    return this.filter { it.isLeader }.map { it.membersInMyBroadcastGroup.size }.sum()
}

fun generateRandomBrokerLocations(brokerNumber: Int): Map<BrokerId, Location> {
    val tmpMap = mutableMapOf<BrokerId, Location>()
    val bounding = Geofence.circle(Location(30.0, 30.0), 5.0)

    repeat(brokerNumber) {
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
