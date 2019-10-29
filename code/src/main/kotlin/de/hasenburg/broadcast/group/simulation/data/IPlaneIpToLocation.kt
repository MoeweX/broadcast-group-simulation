package de.hasenburg.broadcast.group.simulation.data

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import org.apache.logging.log4j.LogManager
import java.io.File
import io.ipinfo.api.IPInfo
import okhttp3.OkHttpClient

private val logger = LogManager.getLogger()

/**
 * Reads data loaded by IPlaneLoader.kt and uses ipinfo.io to generate a CSV file
 * with IP addresses and location info. An ipinfo.io secret is required to run.
 */
fun main(args: Array<String>) {
    // configuration
    val conf = mainBody { ArgParser(args).parseInto(::ConfIpToLocation) }

    // process csv file if it exists to prevent redundant requests
    val outFile = File("${conf.outputDir.absolutePath}/ip_locations.csv")
    val alreadyProcessed = if (!outFile.exists()) {
        emptyList<String>()
    } else {
        logger.info("Found existing ${outFile.name}, processing")
        outFile.readLines()
                .map { s -> s.split(conf.delim)[0] } // get ip address as it is the first entry
                .drop(1) // first line contains header
    }
    if (alreadyProcessed.isNotEmpty()) {
        logger.info("Found ${alreadyProcessed.size} IP addresses in ${outFile.name}")
    }

    // read in all .txt files from inputDir and get their ip addresses
    val toProcess = loadInputFileNames(conf.inputDir)
    val ipAddresses = toProcess.map { file -> getIpAddresses(file) }
            .flatten()
            .filter { s -> !alreadyProcessed.contains(s) } // filter out ips in csv
            .also { logger.info("Found ${it.size} new IP addresses") }

    // fetch information from ipinfo.io and write to csv file (append if exists)
    val client = OkHttpClient.Builder().build() // use own client to have control over closing connections
    val ipInfo = IPInfo.builder().setToken(conf.secret).setClient(client).build()
    if (!outFile.exists()) {
        outFile.writeText("ip_address${conf.delim}country${conf.delim}latitude${conf.delim}longitude\n")
    }
    ipAddresses.forEach { s -> outFile.appendText(makeCsvLine(s, ipInfo, conf.delim)) }

    logger.info("Finished processing IP addresses")
    // close executor service and connection pool of client for faster shutdown
    client.dispatcher().executorService().shutdown()
    client.connectionPool().evictAll()
}

/**
 * Fetches IP information of [ip] using [ipInfo] for lookup and creates a line for a csv file
 */
private fun makeCsvLine(ip: String, ipInfo: IPInfo, delim: String = ","): String {
    logger.info("Looking up info for IP address $ip")
    val resp = ipInfo.lookupIP(ip)
    return resp.ip + delim + resp.countryCode + delim + resp.latitude + delim + resp.longitude + "\n"
}

/**
 * Reads all lines of [inputFile], splits the content on spaces and filters out IP addresses.
 */
private fun getIpAddresses(inputFile: File): List<String> {
    val ipRegex = """\d\d?\d?\.\d\d?\d?\.\d\d?\d?\.\d\d?\d?""".toRegex()

    logger.info("Reading file ${inputFile.name}")
    return inputFile.readLines() // get lines of input file
            .map { line -> line.trim().split(" ") // somehow \\s wouldn't match...
                    .filter { s -> ipRegex.matches(s) } // filter out non ip addresses
            }.flatten() // flatten list to get a list of ip addresses
}

/**
 * Returns which .txt files are present at [inputDir] (with extension).
 */
private fun loadInputFileNames(inputDir: File): List<File> {
    check(inputDir.exists() && inputDir.isDirectory) { "${inputDir.absolutePath} does not exist or is not a directory" }
    val fileList = inputDir.listFiles() ?: error("IO error while listing files in ${inputDir.absolutePath}")

    return fileList.filter { it.extension == "txt" }.also {
        check(it.isNotEmpty()) { "There are no files at ${inputDir.absolutePath}" }
        logger.info("Found ${it.size} file/s in ${inputDir.absolutePath}")
    }.map { File("${inputDir.absolutePath}/${it.name}") }
}

class ConfIpToLocation(parser: ArgParser) {
    val inputDir by parser
            .storing("-i", "--input", help = "local directory containing the input files") { File(this) }
            .default(File("data/iplane"))
            .addValidator {
                if (!value.exists()) {
                    throw InvalidArgumentException("Directory $value does not exist")
                }
                if (!value.isDirectory) {
                    throw InvalidArgumentException("$value is not a directory")
                }
            }

    val outputDir by parser
            .storing("-o", "--output", help = "local directory where the output is written") { File(this) }
            .default(File("data/ip_locations"))
            .addValidator {
                if (!value.exists()) {
                    throw InvalidArgumentException("Directory $value does not exist")
                }
                if (!value.isDirectory) {
                    throw InvalidArgumentException("$value is not a directory")
                }
            }

    val delim by parser
            .storing("--delimiter", help = "delimiter to be used for csv file") { this }
            .default(",")

    val secret by parser
            .storing("-s", "--secret", help = "secret for ipinfo.io") { this }
}