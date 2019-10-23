package de.hasenburg.broadcast.group.simulation.model

data class BroadcastGroup(val leaderIds: BrokerId, val memberIds: List<BrokerId>)