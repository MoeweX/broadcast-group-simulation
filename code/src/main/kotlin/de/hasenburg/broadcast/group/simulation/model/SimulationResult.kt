package de.hasenburg.broadcast.group.simulation.model

import org.apache.logging.log4j.LogManager
import java.io.BufferedWriter
import java.io.File

private val logger = LogManager.getLogger()

class SimulationResult(val brokers: List<Broker>, val nLeaderJoins: Int, val nMemberJoins: Int,
                       val nMemberNotifications: Int, val ticks: Int, val latencyThreshold: Double) {

    fun printBroadcastGroups() {
        for (broker in brokers) {
            if (broker.isLeader) {
                logger.info("${broker.brokerId} has these members: ${broker.membersInMyBroadcastGroup}")
            }
        }
    }

    fun saveExperimentData(dirPath: String = "./simulation-result/", filePrefix: String = "result") {
        val brokerCsvName = "$filePrefix-$latencyThreshold-brokers.csv"
        val infoCsvName = "$filePrefix-$latencyThreshold-info.txt"

        // broker data
        val brokerWriter = prepareWriter(dirPath, brokerCsvName)
        brokerWriter.write("brokerId;latitude;longitude;lcm;leaderId\n")
        for (b in brokers) {
            brokerWriter.write("${b.brokerId.name};${b.location.lat};${b.location.lon};${b.lcm};${b.leaderId.name}\n")
        }
        brokerWriter.flush()
        brokerWriter.close()

        // info data
        val infoWriter = prepareWriter(dirPath, infoCsvName)
        infoWriter.write("ticks: $ticks\n")
        infoWriter.write("numberOfLeaders: ${brokers.numberOfLeaders()}")
        infoWriter.write("nLeaderJoins: $nLeaderJoins\n")
        infoWriter.write("nMemberJoins: $nMemberJoins\n")
        infoWriter.write("nMemberNotifications: $nMemberNotifications\n")
        infoWriter.write("latencyThreshold: $latencyThreshold\n")
        infoWriter.flush()
        infoWriter.close()
    }

    private fun prepareWriter(dirPath: String, fileName: String): BufferedWriter {
        return File("$dirPath/$fileName").also {
            if (!it.exists()) {
                it.parentFile.mkdirs()
                it.createNewFile()
            }
            check(it.isFile) { "Cannot write results as ${it.absolutePath} is not a file." }
            logger.info("Writing results to file ${it.absoluteFile}")
        }.bufferedWriter()
    }


}