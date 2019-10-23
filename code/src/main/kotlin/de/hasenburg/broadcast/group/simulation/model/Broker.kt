package de.hasenburg.broadcast.group.simulation.model

import de.hasenburg.broadcast.group.simulation.msPerKm
import kotlinx.coroutines.channels.Channel
import kotlin.random.Random
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

private val logger = LogManager.getLogger()

class Broker(val brokerId: BrokerId, private val lcm: Int, private val location: Location,
             private val brokerChannels: Map<BrokerId, Channel<BrokerMessage>>, private val latencyThreshold: Double,
             brokerLocations: Map<BrokerId, Location>) {

    // add utility member functions for logger
    private fun Logger.t(msg: String) {
        logger.trace("[$brokerId] $msg")
    }

    private fun Logger.e(msg: String) {
        logger.error("[$brokerId] $msg")
    }

    private var leaderId: BrokerId = brokerId // in the beginning I am in a broadcast group that just comprises myself
    private var newLeaderId: BrokerId =
            brokerId // if other than myLeaderId, broker is currently joining another one
    val isLeader: Boolean
        get() {
            return leaderId == brokerId
        }

    private val brokerMessages = brokerChannels[brokerId] ?: error("Channel for $brokerId not found")

    // if i am a leader, this list contains my members
    private val membersInMyBroadcastGroup = mutableListOf<BrokerId>()

    private val brokerLatenciesInMs =
            brokerLocations.mapValues { location.distanceKmTo(it.value) * msPerKm }

    // as we loop over the channel, we
    private var messageBuffer: BrokerMessage? = null
    private var iStartedMerge: Boolean = false

    /**
     * The different phases in each tick might require state, the state is reset with this function.
     * Also checks whether the broker is in a valid state.
     */
    fun startNewTick() {
        check(brokerMessages.poll() == null)
        messageBuffer = null
        iStartedMerge = false
    }

    suspend fun sendMergeRequest(currentLeaders: List<BrokerId>) {
        // only leaders, and only 10% chance
        if (!isLeader || Random.nextInt(0, 10) > 0) return

        // find the leader with lowest latency
        val targetLeader = currentLeaders
            .map { Pair(it, brokerLatenciesInMs.getOrDefault(it, -1.0)) }
            .minBy { it.second }


        check(targetLeader != null && targetLeader.second > 0.0) {
            "TargetLeader is $targetLeader, this cannot be true; currentLeaders are $currentLeaders"
        }

        if (brokerLatenciesInMs.checkLatency(targetLeader.first)) {
            // send request
            logger.t("Merging with broker $targetLeader")
            iStartedMerge = true
            BrokerMessage.MergeRequest(lcm, membersInMyBroadcastGroup.size, brokerId).send(targetLeader.first)
        } else {
            logger.t("Broker $targetLeader has the lowest other latency, but is above threshold so not merging")
        }

    }

    /**
     * As this method is also sending, it might store a message in the [messageBuffer].
     *
     * Receives and processes all [BrokerMessage.MergeRequest] sent from other brokers. Replies with at least a
     * [BrokerMessage.MergeReply] that contains a [MergeReplyCode], but might also send additional messages.
     *
     * Only the first one is processed, and only if [iStartedMerge] == false; leader is determined by comparing [lcm]
     * and size of [membersInMyBroadcastGroup]:
     *
     * If other is leader:
     * - reply with [MergeReplyCode.IJoin]
     *
     * If we are leader:
     * - reply with [MergeReplyCode.JoinMe]
     *
     * For all messages after the first one -> replies with [MergeReplyCode.BusyTryAgain].
     *
     * TODO: if first message is JoinMe -> all others that would result in JoinMe as well could also be processed
     */
    suspend fun receiveAndProcessMergeRequests() {
        messageBuffer = null
        while (true) {
            val message = brokerMessages.poll()

            if (message == null) {
                logger.t("Stopping to receive MergeRequests as channel is empty")
                return
            }

            check(message is BrokerMessage.MergeReply || message is BrokerMessage.MergeRequest) { "$message is not a MergeReply or MergeRequest" }

            if (message !is BrokerMessage.MergeRequest) {
                logger.t("Stopping to receive MergeRequests as channel now contains different messages")
                messageBuffer = message
                return
            }

            // not first message or I started a merge
            if (messageBuffer != null || iStartedMerge) {
                logger.t("MergeRequest from $brokerId is not the first or I started a merge, " +
                        "replying with ${MergeReplyCode.BusyTryAgain}")
                BrokerMessage.MergeReply(MergeReplyCode.BusyTryAgain, brokerId).send(message.sender)
                continue
            }

            // first message, so needs to be processed properly
            when (message.lcm.compareTo(lcm)) {
                -1 -> {
                    logger.t("$brokerId should join me")
                    BrokerMessage.MergeReply(MergeReplyCode.JoinMe, brokerId).send(message.sender)
                }
                0 -> {
                    if (message.groupSize < membersInMyBroadcastGroup.size) {
                        logger.t("$brokerId should join me")
                        BrokerMessage.MergeReply(MergeReplyCode.JoinMe, brokerId).send(message.sender)
                    } else {
                        logger.t("I join $brokerId")
                        newLeaderId = message.sender
                        BrokerMessage.MergeReply(MergeReplyCode.IJoin, brokerId).send(message.sender)
                    }
                }
                1 -> {
                    logger.t("I join $brokerId")
                    newLeaderId = message.sender
                    BrokerMessage.MergeReply(MergeReplyCode.IJoin, brokerId).send(message.sender)
                }
            }
        }
    }

    /**
     * Potentially, there is a relevant message in the [messageBuffer], which has to be considered.
     *
     * Processes all [BrokerMessage.MergeReply], if there is an [MergeReplyCode.IJoin], sets [newLeaderId] accordingly.
     */
    fun receiveAndProcessMergeReply() {
        // there might be a message in the message buffer
        val message = messageBuffer ?: brokerMessages.poll()
        messageBuffer = null

        if (message == null) {
            logger.t("We did not receive a merge reply")
            return
        }

        check(message is BrokerMessage.MergeReply) { "$message is not a MergeReply" }

        when (message.mergeReplyCode) {
            MergeReplyCode.JoinMe -> logger.t("${message.sender} joins me")
            MergeReplyCode.IJoin -> {
                logger.t("I join $brokerId")
                newLeaderId = message.sender
            }
            MergeReplyCode.BusyTryAgain -> logger.t("${message.sender} is busy")
        }
    }

    /**
     * When [newLeaderId] != [leaderId], a merge is happening that needs to be communicated to members by sending a
     * [BrokerMessage.MergeInfo].
     */
    suspend fun notifyMembersAboutMerge() {
        if (newLeaderId != leaderId) {
            check(isLeader)
            logger.t("Notifying members about merge")

            for (memberId in membersInMyBroadcastGroup) {
                BrokerMessage.MergeInfo(newLeaderId, brokerId).send(memberId)
            }
        }
    }

    /**
     * If we are a leader broker:
     * - clear [membersInMyBroadcastGroup]
     *
     * If we are a member broker:
     * - check whether we got a [BrokerMessage.MergeInfo] from our leader, join if latency permits or create a new group
     *
     * For both at the end (when join necessary):
     * - send [BrokerMessage.JoinInfo] to leader
     * - set [leaderId] to [newLeaderId]
     */
    suspend fun processMergeInfoAndSendJoinInfo() {
        if (isLeader) {
            membersInMyBroadcastGroup.clear()
        } else {
            brokerMessages.poll()?.also {
                check(it is BrokerMessage.MergeInfo) { "$it is not a MergeInfo" }

                // check latency to see whether join is possible
                if (brokerLatenciesInMs.checkLatency(it.newLeaderId)) {
                    logger.t("Joining new leader $leaderId")
                    newLeaderId = it.newLeaderId
                } else {
                    logger.t("Latency to proposed leader would be above the threshold, starting own broadcast group")
                    newLeaderId = brokerId
                    leaderId = brokerId
                    return
                }
            } ?: return //nothing to do as we did not get a MergeInfo
        }

        // do join
        check(newLeaderId != leaderId)
        BrokerMessage.JoinInfo(brokerId).send(newLeaderId)
        leaderId = newLeaderId
    }

    /**
     * Process incoming [BrokerMessage.JoinInfo], only leaders should receive them.
     * Add joining [Broker] to [membersInMyBroadcastGroup]
     */
    suspend fun processJoinInfo() {
        while (true) {
            val message = brokerMessages.poll()

            if (message == null) {
                logger.t("Stopping to receive JoinInfo as channel is empty")
                return
            }

            check(message is BrokerMessage.JoinInfo)
            check(isLeader)

            logger.t("Adding ${message.sender} to my broadcast group")
            membersInMyBroadcastGroup.add(message.sender)
        }
    }

    /*******************************************************************
     * HELPER
     ******************************************************************/

    private fun Map<BrokerId, Double>.checkLatency(brokerId: BrokerId): Boolean {
        return this[brokerId] ?: error("There is no latency for broker $brokerId") < latencyThreshold
    }

    private suspend fun BrokerMessage.send(brokerId: BrokerId) {
        brokerChannels[brokerId]?.send(this) ?: error("There is no channel for broker $brokerId")
    }
}

data class BrokerId(val name: String)