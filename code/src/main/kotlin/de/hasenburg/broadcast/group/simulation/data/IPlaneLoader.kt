@file:Suppress("SameParameterValue")

package de.hasenburg.broadcast.group.simulation.data

import kotlinx.coroutines.*
import org.apache.logging.log4j.LogManager
import java.io.File
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import java.lang.NumberFormatException
import kotlin.random.Random


private val logger = LogManager.getLogger()

fun main() {
    // configuration
    val conf = object {
        // to define
        val localDataDir = "data/iplane/"
        private val dataUrl = "https://web.eecs.umich.edu/~harshavm/iplane/iplane_logs/data"
        private val year = "2016"
        private val months = listOf("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")

        // computed
        val targetUrls = months.map { "$dataUrl/$year/$it" }
    }

    // logic
    val existingFiles = loadExistingLocalFileNames(conf.localDataDir)
    for ((i, targetUrl) in conf.targetUrls.withIndex()) {
        logger.info("Processing target URL ${i+1}/${conf.targetUrls.size}")
        val remoteFileUrls = getRemoteFileUrls(targetUrl, existingFiles)
        downloadFiles(conf.localDataDir, remoteFileUrls)
    }

    logger.info("All files have been downloaded")
}

private fun loadExistingLocalFileNames(localDataDir: String): List<String> {
    val file = File(localDataDir)
    check(file.exists() && file.isDirectory) { "${file.absolutePath} does not exist or is not a directory" }
    val fileList = file.listFiles() ?: error("IO error while listing files in ${file.absolutePath}")

    return fileList.filter { it.extension == "txt" }.also {
        check(it.isNotEmpty()) { "There are no files at ${file.absolutePath}" }
        logger.info("Found ${it.size} files in ${file.absolutePath}")
    }.map { it.nameWithoutExtension }
}

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

private fun getFilePrefix(url: String) = url.split("data/")[1]
    .replace("/", "").replace("pl_latencies.txt", "")

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

@Suppress("BlockingMethodInNonBlockingContext")
fun downloadFiles(localDataDir: String, remoteFileUrls: List<String>) {
    runBlocking {
        for (remoteFileUrl in remoteFileUrls) {
            launch(Dispatchers.Default) {
                val fileName = "${getFilePrefix(remoteFileUrl)}-pl_latencies.txt"
                val doc = Jsoup.connect(remoteFileUrl).get().body()
                val content = doc.wholeText()
                logger.info("Storing ${content.length} characters in $fileName")
                File("$localDataDir/$fileName").writeText(content)
            }
            delay(Random.nextLong(500, 2000)) // waiting is sometimes a good idea
        }
    }
}
