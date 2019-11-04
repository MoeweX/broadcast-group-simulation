package de.hasenburg.broadcast.group.simulation.model

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import me.tongfei.progressbar.ProgressBar
import org.apache.logging.log4j.LogManager
import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LogManager.getLogger()

suspend fun runSimulation(latencyThreshold: Double,
                          brokerLocations: Map<BrokerId, Location> = generateRandomBrokerLocations(10),
                          brokerLcms: Map<BrokerId, Int> = brokerLocations.generateRandomBrokerLcms(5))
        : SimulationResult = coroutineScope {

    val brokerChannels = generateBrokerChannel(brokerLocations.keys)
    val brokerJobs = mutableListOf<Deferred<Broker>>()
    val brokerInitChannel = Channel<Int>(Channel.BUFFERED)

    logger.info("Starting to setup brokers.")
    launch { updateProgressBar(brokerInitChannel, brokerLocations.size.toLong()) }

    for ((brokerId, location) in brokerLocations) {
        brokerJobs.add(async(Dispatchers.Default) {
            val b = Broker(brokerId,
                    brokerLcms[brokerId] ?: error("There is no lcm for broker $brokerId"),
                    location,
                    brokerChannels,
                    latencyThreshold,
                    brokerLocations)
            brokerInitChannel.send(1)
            b
        })
    }

    // result fields
    val brokers = brokerJobs.awaitAll()
    var tick = 0
    val nLeaderJoins = AtomicInteger(0)
    val nMemberNotifications = AtomicInteger(0)
    val nMemberJoins = AtomicInteger(0)
    logger.info("All brokers ready")

    do {
        logger.trace("\n\n")
        logger.debug("Starting tick ${++tick}, currently there are ${brokers.numberOfLeaders()} leader")
        coroutineScope {
            brokers.forEach { launch(Dispatchers.Default) { it.startNewTick() } }
        }

        logger.trace("Sending MergeRequests")
        val leaders = brokers.leaders()
        coroutineScope {
            brokers.forEach { launch(Dispatchers.Default) { it.sendMergeRequest(leaders) } }
        }

        logger.trace("Receiving MergeRequests and Sending MergeReply")
        coroutineScope {
            brokers.forEach { launch(Dispatchers.Default) { it.receiveAndProcessMergeRequests() } }
        }

        logger.trace("Receiving MergeReply")
        coroutineScope {
            brokers.forEach { launch(Dispatchers.Default) { it.receiveMergeReply() } }
        }

        logger.trace("Notifying members about merge")
        coroutineScope {
            brokers.forEach {
                launch(Dispatchers.Default) {
                    nMemberNotifications.addAndGet(it.notifyMembersAboutMerge())
                }
            }
        }

        logger.trace("Leaders and Members are merging")
        coroutineScope {
            brokers.forEach {
                launch(Dispatchers.Default) {
                    when (it.doJoin()) {
                        JoinType.JoinLeader -> nLeaderJoins.incrementAndGet()
                        JoinType.JoinMember -> nMemberJoins.incrementAndGet()
                        JoinType.NoJoin -> {
                        }
                    }
                }
            }
        }

        logger.trace("Receiving JoinInfo")
        coroutineScope {
            brokers.forEach { launch(Dispatchers.Default) { it.receiveJoinInfo() } }
        }

    } while (!brokers.validateCreatedBroadcastGroups(false))

    logger.info("Stable state reached after $tick ticks, there are ${brokers.numberOfLeaders()} leaders")
    check(brokers.size == brokers.numberOfMembers() + brokers.numberOfLeaders())
    check(brokers.validateCreatedBroadcastGroups())
    SimulationResult(brokers,
            nLeaderJoins.get(),
            nMemberJoins.get(),
            nMemberNotifications.get(),
            tick,
            latencyThreshold)
}

suspend fun updateProgressBar(brokerInitChannel: Channel<Int>, amount: Long) {

    val pb = ProgressBar("Broker Initialization", amount)

    for (i in brokerInitChannel) {
        pb.step()

        if (pb.current == amount) {
            brokerInitChannel.close()
        }
    }
    pb.close()
    println()

}

/**
 * Broadcast groups are valid, if:
 * - all members have a latency to their leader below the defined threshold
 * - a leader has a latency to all other leaders above the defined threshold
 */
private fun List<Broker>.validateCreatedBroadcastGroups(log: Boolean = true): Boolean {
    for (broker in this) {
        if (!broker.validateLatency(this.leaders(), log)) {
            return false
        }
    }

    return true
}

private fun List<Broker>.leaders(): List<BrokerId> {
    return this.filter { it.isLeader }.map { it.brokerId }
}

fun List<Broker>.numberOfLeaders(): Int {
    return this.filter { it.isLeader }.size
}

private fun List<Broker>.numberOfMembers(): Int {
    return this.filter { it.isLeader }.map { it.membersInMyBroadcastGroup.size }.sum()
}

fun generateRandomBrokerLocations(brokerNumber: Int): Map<BrokerId, Location> {
    val tmpMap = mutableMapOf<BrokerId, Location>()

    val center = Location(30.0, 30.0)

    repeat(brokerNumber) {
        tmpMap[BrokerId("Broker-$it")] = center.otherInDistance(
                Random.nextDouble(0.0, 5000.0), Random.nextDouble(0.0, 360.0))
    }

    return tmpMap
}

fun Map<BrokerId, Location>.generateRandomBrokerLcms(upperBound: Int): Map<BrokerId, Int> {
    return this.mapValues { Random.nextInt(0, upperBound) }
}

private fun generateBrokerChannel(brokerIds: Set<BrokerId>): Map<BrokerId, Channel<BrokerMessage>> {
    val tmpMap = mutableMapOf<BrokerId, Channel<BrokerMessage>>()

    for (brokerId in brokerIds) {
        tmpMap[brokerId] = Channel(Channel.UNLIMITED)
    }

    return tmpMap
}
