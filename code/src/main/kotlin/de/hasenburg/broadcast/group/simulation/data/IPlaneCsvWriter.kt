package de.hasenburg.broadcast.group.simulation.data

import org.apache.logging.log4j.LogManager
import java.io.File
import io.ipinfo.api.IPInfo

private val logger = LogManager.getLogger()

fun main(args: Array<String>) {
    // configuration
    // TODO: maybe expose more via command line args
    val conf = object {
        // to define
        val secret = if (args.isNotEmpty()) args[0] else error("Secret was not provided.")
        val delim = "," // delimiter for csv files
        val inputDir = "data/iplane/"
        val outputDir = "data/ip_locations/"
    }
    logger.info("Starting")

    // process csv file if it exists to prevent redundant requests
    val outFile = File(conf.outputDir + "ip_locations.csv")
    val alreadyProcessed = if (!outFile.exists()) {
        emptyList<String>()
    } else {
        logger.info("Found " + outFile.name + ", processing")
        outFile.readLines()
                .map { s -> s.split(conf.delim)[0] } // get ip address as it is the first entry
                .drop(1) // first line contains header
    }

    // read in all .txt files from inputDir and get their ip addresses
    val toProcess = loadInputFileNames(conf.inputDir)
    val ipAddresses = toProcess.map { file -> getIpAddresses(File(conf.inputDir + file)) }
            .flatten()
            .filter { s -> !alreadyProcessed.contains(s) } // filter out ips in csv

    // fetch information from ipinfo.io and write to csv file (append if exists)
    val ipInfo = IPInfo.builder().setToken(conf.secret).build()
    if (!outFile.exists()) {
        outFile.writeText("ip_address" + conf.delim + "country" + conf.delim + "latitude" + conf.delim + "longitude\n")
    }
    ipAddresses.forEach { s -> outFile.appendText(makeCsvLine(s, ipInfo, conf.delim)) }

    logger.info("Finished")
}

/**
 * Fetches IP information of [ip] using [ipInfo] for lookup and creates a line for a csv file
 */
private fun makeCsvLine(ip: String, ipInfo: IPInfo, delim: String = ","): String {
    logger.info("Looking up info for IP address " + ip)
    val resp = ipInfo.lookupIP(ip)
    return resp.ip + delim + resp.countryCode + delim + resp.latitude + delim + resp.longitude + "\n"
}

/**
 * Reads all lines of [inputFile], splits the content on spaces and filters out IP addresses.
 */
private fun getIpAddresses(inputFile: File): List<String> {
    val ipRegex = """\d\d?\d?\.\d\d?\d?\.\d\d?\d?\.\d\d?\d?""".toRegex()

    logger.info("Reading file " + inputFile.name)
    val ipList = inputFile.readLines()               // get lines of input file
            .map { line -> line.trim().split(" ") // somehow \\s wouldn't match...
                    .filter { s -> ipRegex.matches(s) }     // filter out non ip addresses
            }.flatten()        // flatten list to get a list of ip addresses
    return ipList
}

/**
 * Returns which .txt files are present at [inputDir] (with extension).
 */
private fun loadInputFileNames(inputDir: String): List<String> {
    val file = File(inputDir)
    check(file.exists() && file.isDirectory) { "${file.absolutePath} does not exist or is not a directory" }
    val fileList = file.listFiles() ?: error("IO error while listing files in ${file.absolutePath}")

    return fileList.filter { it.extension == "txt" }.also {
        logger.info("Found ${it.size} file/s in ${file.absolutePath}")
    }.map { it.name }
}