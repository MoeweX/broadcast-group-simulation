package de.hasenburg.broadcast.group.simulation.model

import de.hasenburg.broadcast.group.simulation.msPerKm
import kotlinx.coroutines.channels.Channel
import kotlin.random.Random
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

private val logger = LogManager.getLogger()

class Broker(val brokerId: BrokerId, private val lcm: Int,
             private val location: Location,
             private val brokerChannels: Map<BrokerId, Channel<BrokerMessage>>,
             private val latencyThreshold: Double,
             brokerLocations: Map<BrokerId, Location>) {

    // add utility member functions for logger
    private val b = "[${brokerId.name}]"

    private fun Logger.t(msg: String) {
        this.trace("$b $msg")
    }

    private fun Logger.d(msg: String) {
        this.debug("$b $msg")
    }

    private fun Logger.w(msg: String) {
        this.warn("$b $msg")
    }

    private fun Logger.e(msg: String) {
        this.error("$b $msg")
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
    val membersInMyBroadcastGroup = mutableListOf<BrokerId>()

    private val brokerLatenciesInMs =
            brokerLocations.mapValues { location.distanceKmTo(it.value) * msPerKm }

    // as we loop over the channel, we
    private var messageBuffer: BrokerMessage? = null
    private var busyAsMerging: Boolean = false

    /**
     * The different phases in each tick might require state, the state is reset with this function.
     * Also checks whether the broker is in a valid state.
     */
    fun startNewTick() {
        check(brokerMessages.poll() == null)
        messageBuffer = null
        busyAsMerging = false
    }

    suspend fun sendMergeRequest(currentLeaders: List<BrokerId>) {
        // only leaders, and only 10% chance
        if (!isLeader || Random.nextInt(0, 10) > 2) return

        // find the leader with lowest latency
        val targetLeader = currentLeaders
            .filter { it != brokerId }
            .map { Pair(it, brokerLatenciesInMs.getOrDefault(it, -1.0)) }
            .minBy { it.second }


        check(targetLeader != null && targetLeader.second > 0.0) {
            "TargetLeader is $targetLeader, this cannot be true; currentLeaders are $currentLeaders"
        }

        if (brokerLatenciesInMs.checkLatency(targetLeader.first)) {
            // send request
            logger.d("Sending MergeRequest to broker $targetLeader")
            busyAsMerging = true
            BrokerMessage.MergeRequest(lcm, membersInMyBroadcastGroup.size, brokerId).send(targetLeader.first)
        } else {
            logger.d("Broker $targetLeader has the lowest other latency, but is above threshold so not merging")
        }

    }

    /**
     * As this method is also sending, it might store a message in the [messageBuffer].
     *
     * Receives and processes all [BrokerMessage.MergeRequest] sent from other brokers. Replies with at least a
     * [BrokerMessage.MergeReply] that contains a [MergeReplyCode], but might also send additional messages.
     *
     * If [busyAsMerging] == false; leader is determined by comparing [lcm] and size of [membersInMyBroadcastGroup]:
     *
     * ... and if other is leader:
     * - reply with [MergeReplyCode.IJoin]
     *
     * ... and if we are leader:
     * - reply with [MergeReplyCode.JoinMe]
     *
     * If [busyAsMerging] == true -> replies with [MergeReplyCode.BusyTryAgain].
     *
     * TODO: if I did not start the merge and first reply is JoinMe -> all others that would result in JoinMe could be answered as well
     */
    suspend fun receiveAndProcessMergeRequests() {
        check(messageBuffer == null)
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

            // already merging
            if (busyAsMerging) {
                logger.d("Busy merging, so replying with ${MergeReplyCode.BusyTryAgain} to ${message.sender}")
                BrokerMessage.MergeReply(MergeReplyCode.BusyTryAgain, brokerId).send(message.sender)
                continue
            }

            // first message, so needs to be processed properly
            busyAsMerging = true
            when (message.lcm.compareTo(lcm)) {
                -1 -> {
                    logger.d("${message.sender} should join me")
                    BrokerMessage.MergeReply(MergeReplyCode.JoinMe, brokerId).send(message.sender)
                }
                0 -> {
                    if (message.groupSize < membersInMyBroadcastGroup.size) {
                        logger.d("${message.sender} should join me")
                        BrokerMessage.MergeReply(MergeReplyCode.JoinMe, brokerId).send(message.sender)
                    } else {
                        logger.d("I join ${message.sender}")
                        newLeaderId = message.sender
                        BrokerMessage.MergeReply(MergeReplyCode.IJoin, brokerId).send(message.sender)
                    }
                }
                1 -> {
                    logger.d("I join ${message.sender}")
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
    fun receiveMergeReply() {
        // there might be a message in the message buffer
        val message = messageBuffer ?: brokerMessages.poll()
        messageBuffer = null

        if (message == null) {
            logger.t("We did not receive a merge reply")
            // TODO we could add a check() here, this should only happen if we did not send a MergeRequest
            return
        }

        check(message is BrokerMessage.MergeReply) { "$message is not a MergeReply" }

        when (message.mergeReplyCode) {
            // these code are from the perspective of the sender
            MergeReplyCode.JoinMe -> {
                logger.d("I join ${message.sender}")
                newLeaderId = message.sender
            }
            MergeReplyCode.IJoin -> {
                logger.d("${message.sender} joins me")
            }
            MergeReplyCode.BusyTryAgain -> logger.d("${message.sender} is busy")
        }
    }

    /**
     * When [newLeaderId] != [leaderId], a merge is happening that needs to be communicated to members by sending a
     * [BrokerMessage.MergeInfo].
     */
    suspend fun notifyMembersAboutMerge() {
        if (newLeaderId != leaderId) {
            check(isLeader) { "$b Should have been a leader" }
            if (membersInMyBroadcastGroup.isEmpty()) {
                logger.d("No members to notify about me joining $newLeaderId, broadcast group is empty.")
                return
            }
            logger.d("Notifying members about me joining $newLeaderId")
            for (memberId in membersInMyBroadcastGroup) {
                logger.d("Notifying $memberId about me joining $newLeaderId")
                BrokerMessage.MergeInfo(newLeaderId, brokerId).send(memberId)
            }
        }
    }

    /**
     * Does the merge.
     * As this method is also sending, it might store a message in the [messageBuffer].
     *
     * If we are a leader broker:
     * - check if [newLeaderId] != [leaderId], if so -> clear [membersInMyBroadcastGroup]
     *
     * If we are a member broker:
     * - check whether we got a [BrokerMessage.MergeInfo] from our leader, join if latency permits or create a new group
     *
     * For both at the end (when join necessary):
     * - send [BrokerMessage.JoinInfo] to leader
     * - set [leaderId] to [newLeaderId]
     */
    suspend fun doMerge() {
        check(messageBuffer == null)
        if (newLeaderId != leaderId) {
            check(isLeader) { "$b New leader information should only be set on leaders, yet" }
            logger.d("Was leader, now joining other leader $newLeaderId")
            membersInMyBroadcastGroup.clear()
        } else {
            brokerMessages.poll()?.also {
                if (it is BrokerMessage.JoinInfo) {
                    messageBuffer = it
                    logger.t("$b Found a JoinInfo in channel, preserving it for later.")
                    return
                }

                check(it is BrokerMessage.MergeInfo) { "$it is not a MergeInfo" }

                // check latency to see whether join is possible
                if (brokerLatenciesInMs.checkLatency(it.newLeaderId)) {
                    logger.d("Was member of ${leaderId}, now joining other leader ${it.newLeaderId}")
                    newLeaderId = it.newLeaderId
                } else {
                    logger.d("Latency to proposed leader would be above the threshold, starting own broadcast group")
                    newLeaderId = brokerId
                    leaderId = brokerId
                    return
                }
            } ?: run {
                logger.t("Did not receive a MergeInfo, so no merge needed")
                return
            }

        }

        // send join info if we should merge
        check(newLeaderId != leaderId) // we only came here if we merge, all other cases return
        BrokerMessage.JoinInfo(brokerId).send(newLeaderId)
        leaderId = newLeaderId
    }

    /**
     * Process incoming [BrokerMessage.JoinInfo], only leaders should receive them.
     * Add joining [Broker] to [membersInMyBroadcastGroup]
     */
    fun receiveJoinInfo() {
        while (true) {
            val message = messageBuffer ?: brokerMessages.poll()
            messageBuffer = null

            if (message == null) {
                logger.t("Stopping to receive JoinInfo as channel is empty")
                return
            }

            check(message is BrokerMessage.JoinInfo)
            check(isLeader) { "$b I should be a leader" }

            logger.d("Adding ${message.sender} to my broadcast group")
            membersInMyBroadcastGroup.add(message.sender)
        }
    }

    /*******************************************************************
     * HELPER
     ******************************************************************/

    /**
     * For member -> check whether to leader is below the defined threshold
     *
     * For leader -> check whether latency to other leaders is below the defined threshold
     * Broadcast groups are valid, if:
     * - all members have a latency to their leader below the defined threshold
     * - a leader has a latency to all other leaders above the defined threshold
     */
    fun validateLatency(leaders: List<BrokerId>, log: Boolean = true): Boolean {
        if (isLeader) {
            for (otherLeaderId in leaders.filter { it != brokerId }) {
                if (brokerLatenciesInMs.checkLatency(otherLeaderId)) {
                    if (log) logger.w("Latency to other leader $otherLeaderId is below threshold, no valid state reached")
                    return false
                }
            }
        } else {
            if (!brokerLatenciesInMs.checkLatency(leaderId)) {
                if (log) logger.w("Latency to my leader $leaderId is above threshold, no valid state reached")
                return false
            }
        }
        return true
    }

    private fun Map<BrokerId, Double>.checkLatency(brokerId: BrokerId): Boolean {
        return this[brokerId] ?: error("There is no latency for broker $brokerId") < latencyThreshold
    }

    private suspend fun BrokerMessage.send(brokerId: BrokerId) {
        brokerChannels[brokerId]?.send(this) ?: error("There is no channel for broker $brokerId")
    }

    override fun toString(): String {
        return "Broker(brokerId=$brokerId, lcm=$lcm, location=$location, latencyThreshold=$latencyThreshold, " +
                "leaderId=$leaderId, newLeaderId=$newLeaderId, membersInMyBroadcastGroup=$membersInMyBroadcastGroup, " +
                "brokerLatenciesInMs=$brokerLatenciesInMs, messageBuffer=$messageBuffer, iStartedMerge=$busyAsMerging)"
    }

}

data class BrokerId(val name: String)