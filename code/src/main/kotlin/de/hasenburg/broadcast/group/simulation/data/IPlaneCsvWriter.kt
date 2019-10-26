package de.hasenburg.broadcast.group.simulation.data

import org.apache.logging.log4j.LogManager
import java.io.File
import java.util.stream.Collectors
import io.ipinfo.api.IPInfo

private val logger = LogManager.getLogger()

fun main(args: Array<String>) {
    // configuration
    val conf = object {
        // to define
        val inputDir = "data/iplane/"
        val outputDir = "data/ip_locations"
        val secret = if (args.isNotEmpty()) args[0] else error("Secret was not provided.")
    }

    // TODO: read csvs already created

    //val toProcess = loadInputFileNames(conf.inputDir)
    //val ipAddresses = toProcess.map { file -> getIpAddresses(File(conf.inputDir + file)) }.flatten()

//    val ipInfo = IPInfo.builder().setToken(conf.secret).build()
//    val response = ipInfo.lookupIP("")
//    println(response)
}

private fun getIpAddresses(inputFile: File): List<String> {
    val ipRegex = """\d\d?\d?\.\d\d?\d?\.\d\d?\d?\.\d\d?\d?""".toRegex()
    return inputFile.bufferedReader().lines()               // get lines of input file
            .map { line -> line.trim().split(" ") // somehow \\s wouldn't match...
                    .filter { s -> ipRegex.matches(s) }     // filter out non ip addresses
            }.collect(Collectors.toList()).flatten()        // flatten list to get a list of ip addresses
}

/**
 * Returns which .txt files are present at [inputDir] (with extension).
 */
private fun loadInputFileNames(inputDir: String): List<String> {
    val file = File(inputDir)
    check(file.exists() && file.isDirectory) { "${file.absolutePath} does not exist or is not a directory" }
    val fileList = file.listFiles() ?: error("IO error while listing files in ${file.absolutePath}")

    return fileList.filter { it.extension == "txt" }.also {
        logger.info("Found ${it.size} files in ${file.absolutePath}")
    }.map { it.name }
}