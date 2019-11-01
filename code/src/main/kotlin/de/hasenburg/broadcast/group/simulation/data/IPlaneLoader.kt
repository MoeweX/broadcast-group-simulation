@file:Suppress("SameParameterValue")

package de.hasenburg.broadcast.group.simulation.data

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.InvalidArgumentException
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import kotlinx.coroutines.*
import org.apache.logging.log4j.LogManager
import org.jsoup.HttpStatusException
import java.io.File
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.lang.NumberFormatException
import java.text.DecimalFormat
import kotlin.random.Random


private val logger = LogManager.getLogger()

/**
 * Loads iPlane files from the defined urls and stores them in the given data dir.
 * Only loads files not present in the dir, already.
 *
 * For more information on iPlane data, see https://web.eecs.umich.edu/~harshavm/iplane/
 */
fun main(args: Array<String>) {
    // configuration
    val conf = mainBody { ArgParser(args).parseInto(::Conf) }

    // logic
    val existingFiles = loadExistingLocalFileNames(conf.localDataDir)
    for ((i, targetUrl) in conf.targetUrls.withIndex()) {
        logger.info("Processing target URL ${i + 1}/${conf.targetUrls.size}")
        val remoteFileUrls = getRemoteFileUrls(targetUrl, existingFiles)
        downloadFiles(conf.localDataDir, remoteFileUrls)
    }

    logger.info("All files have been downloaded")
}

/**
 * Returns which .txt files are present at [localDataDir].
 */
private fun loadExistingLocalFileNames(localDataDir: File): List<String> {
    val fileList = localDataDir.listFiles() ?: error("IO error while listing files in ${localDataDir.absolutePath}")

    return fileList.filter { it.extension == "txt" }.also {
        logger.info("Found ${it.size} files in ${localDataDir.absolutePath}")
    }.map { it.nameWithoutExtension }
}

/**
 * Returns individual file urls for the given [targetUrl] (should target the data set's month website).
 * Returns only urls to files that do not exist in [existingFiles], yet.
 */
private fun getRemoteFileUrls(targetUrl: String, existingFiles: List<String>): List<String> {
    val doc = Jsoup.connect(targetUrl).get()
    val dayUrls = getDayUrls(targetUrl, doc).map { "$it/pl_latencies.txt" }
    // only keep urls for files that are not stored locally, yet
    val filePrefixes = existingFiles.map { it.split("-")[0] }
    return dayUrls.filter {
        val prefix = getFilePrefix(it)
        !filePrefixes.contains(prefix)
    }.also { logger.info("${it.size}/${dayUrls.size} files found at $targetUrl are not available locally, yet") }
}

/**
 * Gets the date prefix for a given url, for example:
 * https://web.eecs.umich.edu/~harshavm/iplane/iplane_logs/data/2016/08/01/pl_latencies.txt -> 20160801
 */
private fun getFilePrefix(url: String) = url.split("data/")[1]
    .replace("/", "").replace("pl_latencies.txt", "")

/**
 * Helper method for [getRemoteFileUrls] to identify which day urls exist for a given [monthBody].
 */
private fun getDayUrls(targetUrl: String, monthBody: Document): List<String> {
    val links = monthBody.select("a")
    return links.filter {
        try {
            it.text().replace("/", "").toInt()
            true
        } catch (e: NumberFormatException) {
            false
        }
    }.map { "$targetUrl/${it.text()}" }
}

/**
 * Downloads all files found at [remoteFileUrls] and stores them in [localDataDir].
 */
@Suppress("BlockingMethodInNonBlockingContext")
fun downloadFiles(localDataDir: File, remoteFileUrls: List<String>) {
    runBlocking {
        for (remoteFileUrl in remoteFileUrls) {
            launch(Dispatchers.IO) {
                // download files concurrently
                val fileName = "${getFilePrefix(remoteFileUrl)}-pl_latencies.txt"
                try {
                    val doc = Jsoup.connect(remoteFileUrl).get().body()
                    val content = doc.wholeText()
                    logger.info("Storing ${content.length} characters in $fileName")
                    File("${localDataDir.absolutePath}/$fileName").writeText(content)
                } catch (e: HttpStatusException) {
                    logger.warn("Could not fetch $remoteFileUrl")
                }
            }
            delay(Random.nextLong(0, 1)) // waiting is sometimes a good idea
        }
    } // this waits until all launched download coroutines are done
}

class Conf(parser: ArgParser) {
    val localDataDir by parser
        .storing("-d", "--dir", help = "local directory which stores loaded data") { File(this) }
        .default(File("data/iplane"))
        .addValidator {
            if (!value.exists()) {
                throw InvalidArgumentException("Directory $value does not exist")
            }
            if (!value.isDirectory) {
                throw InvalidArgumentException("$value is not a directory")
            }
        }

    private val dataUrl = "https://web.eecs.umich.edu/~harshavm/iplane/iplane_logs/data"
    private val years by parser
        .storing("-y", "--years", help = "years the data was collected, e.g., 2014,2015,2016") { this.split(",").map { it.toInt() } }
        .default(listOf(2016))
    private val months by parser
        .storing("-m", "--months", help = "months the data was collected, e.g., 1 or 1,2,3") {
            this.split(",").map { DecimalFormat("00").format(it.toInt()) }
        }.default(listOf("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"))
        .addValidator {
            for (month in value) {
                if (month.toInt() < 1 || month.toInt() > 12) {
                    throw InvalidArgumentException("Month parameters must be between 1 and 12, was $month")
                }
            }
        }

    val targetUrls = years.map {
        val year = it
        months.map { "$dataUrl/$year/$it" }
    }.flatten()
}
