package de.hasenburg.broadcast.group.simulation.model

sealed class BrokerMessage {
    abstract val sender: BrokerId

    /**
     * Send from one leader [Broker] to another to initiate a group merge.
     */
    data class MergeRequest(val lcm: Int, val groupSize: Int, override val sender: BrokerId) : BrokerMessage()

    /**
     * Every leader [Broker] replies with this message to each received [MergeRequest].
     */
    data class MergeReply(val mergeReplyCode: MergeReplyCode, override val sender: BrokerId) : BrokerMessage()

    /**
     * Send from a leader [Broker] to all of its member [Broker] when joining the [BroadcastGroup] of
     * another leader [Broker].
     *
     * Upon reception, each member [Broker] will join the described leader; no confirmation is necessary.
     */
    data class MergeInfo(val newLeaderId: BrokerId, override val sender: BrokerId) : BrokerMessage()

    /**
     * Send from a [Broker] to a leader [Broker] when joining its [BroadcastGroup]. The leader [Broker] will add the
     * sending [Broker] to its [BroadcastGroup]; no confirmation is necessary.
     */
    data class JoinInfo(override val sender: BrokerId) : BrokerMessage()
}

/**
 * Reply codes for [BrokerMessage.MergeReply].
 *
 * - [JoinMe]: the other leader [Broker] should join the [BroadcastGroup] of the sending leader [Broker]
 * - [IJoin]: the sending leader [Broker] will join the [BroadcastGroup] of the other leader [Broker]
 * - [BusyTryAgain]: the sending leader [Broker] is already negotiating a merge with another leader [Broker]
 */
enum class MergeReplyCode {
    JoinMe,
    IJoin,
    BusyTryAgain
}

/**
 * Return type of [Broker.doJoin] to indicate whether a joined occured, and whether the join is carried out by a
 * member or a leader [Broker].
 */
enum class JoinType {
    JoinLeader,
    JoinMember,
    NoJoin
}
