package de.hasenburg.broadcast.group.simulation.data

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import de.hasenburg.broadcast.group.simulation.model.Location
import me.tongfei.progressbar.ProgressBar
import org.apache.logging.log4j.LogManager
import org.nield.kotlinstatistics.median
import org.nield.kotlinstatistics.variance
import java.io.BufferedWriter
import java.io.File

private val logger = LogManager.getLogger()

fun main(args: Array<String>) {
    // configuration
    val conf = mainBody { ArgParser(args).parseInto(::ConfEnricher) }

    // logic
    val ips = readInIps(conf.ipFile)
    val msKms = enrichIplaneFiles(conf.dirIplane, conf.output, conf.years, ips)

    logger.info("There are ${msKms.size} communications")
    logger.info("Median is ${msKms.median()}")
}


/* ************************************************
   ******* IP File ********************************
   ************************************************ */

private fun readInIps(ipFile: File): Map<String, IP> {
    return ipFile.readLines().drop(1).map { it.getIp() }.map { it.ip to it }.toMap()
}

private data class IP(val ip: String, val country: String, val lat: Double, val lon: Double)

/**
 * Might throw a [NumberFormatException].
 */
private fun String.getIp(): IP {
    val split = this.split(",")
    check(split.size == 4) { "$this is not a valid ip row" }
    return IP(split[0], split[1], split[2].toDouble(), split[3].toDouble())
}

/* ************************************************
   ******* Iplane Files ***************************
   ************************************************ */

private data class IPlaneRow(val ip1: String, val ip2: String, val latency: Int)

/**
 * Might throw a [NumberFormatException].
 */
private fun String.getIPlaneRow(): IPlaneRow {
    val split = this.split(" ")
    check(split.size == 3) { "$this is not a valid iplane row" }
    return IPlaneRow(split[0], split[1], split[2].toInt())
}

/**
 * @return list of ms/km values
 */
private fun enrichIplaneFiles(dirIplane: File, output: File, years: List<Int>, ips: Map<String, IP>): List<Double> {
    // determine files
    val files = dirIplane.listFiles()?.filter {
        var keep = false
        // must contain any year
        for (year in years) {
            if (it.nameWithoutExtension.contains(year.toString())) {
                keep = true
                break
            }
        }
        keep
    } ?: error("There are no files at ${dirIplane.absolutePath}")
    logger.info("Using ${files.size} files as they contain data from the year(s) $years")

    // iterate over file contents, enrich, write to output
    val progress = ProgressBar("Processing files", files.size.toLong())
    val writer = output.bufferedWriter()
    val msKms = mutableListOf<Double>()
    writer.writeln("ip1,ip2,country1,country2,lat1,lon1,lat2,lon2,latency[ms],distance[km],ms/km")
    for (file in files) {
        file.forEachLine {
            val ipr = it.getIPlaneRow()
            val ip1 = ips[ipr.ip1] ?: error("Missing location for ip ${ipr.ip1}")
            val ip2 = ips[ipr.ip2] ?: error("Missing location for ip ${ipr.ip2}")
            val distance = Location(ip1.lat, ip1.lon).distanceKmTo(Location(ip2.lat, ip2.lon))
            val msKm = ipr.latency / distance
            msKms.add(msKm)

            writer.writeln("${ip1.ip},${ip2.ip},${ip1.country},${ip2.country}," +
                    "${ip1.lat},${ip1.lon},${ip2.lat},${ip2.lon},${ipr.latency},$distance,$msKm")
        }
        progress.step()
    }
    writer.flush()
    writer.close()
    progress.close()

    return msKms
}

fun BufferedWriter.writeln(line: String) {
    this.write(line)
    this.newLine()
}

/* ************************************************
   ******* Conf ***********************************
   ************************************************ */

class ConfEnricher(parser: ArgParser) {
    val dirIplane by parser
        .storing("--iplane", help = "local iplane directory") { File(this) }
        .default(File("data/iplane"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("Directory $value does not exist")
            }
            if (!value.isDirectory) {
                throw InvalidArgumentException("$value is not a directory")
            }
        }

    val ipFile by parser
        .storing("--ips", help = "file that stores ip locations") { File(this) }
        .default(File("data/ip_locations/ip_locations.csv"))
        .addValidator {
            if (!value.parentFile.exists()) {
                throw InvalidArgumentException("Directory ${value.parentFile} does not exist")
            }
            if (!value.parentFile.isDirectory) {
                throw InvalidArgumentException("${value.parentFile} is not a directory")
            }
        }

    val output by parser
        .storing("-o", "--output", help = "file to store output data") { File(this) }
        .default(File("data/enriched/enriched.csv"))
        .addValidator {
            if (!value.parentFile.exists()) {
                throw InvalidArgumentException("Directory ${value.parentFile} does not exist")
            }
            if (!value.parentFile.isDirectory) {
                throw InvalidArgumentException("${value.parentFile} is not a directory")
            }
            if (value.exists()) {
                logger.info("${value.absoluteFile} already exists, overwriting it.")
            }
        }

    val years by parser
        .storing("-y", "--years", help = "iplane years to be considered, e.g., 2015,2016") {
            this.split(",")
                .map { it.toInt() }
        }
        .default(listOf(2016))

}