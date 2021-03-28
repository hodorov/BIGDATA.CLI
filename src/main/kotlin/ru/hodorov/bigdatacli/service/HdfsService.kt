package ru.hodorov.bigdatacli.service

import mu.KotlinLogging
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask
import java.util.stream.Stream

private val log = KotlinLogging.logger { }

@Service
class HdfsService(
    private val fs: FileSystem,
    private val fjp: ForkJoinPool
) {

    // Stream (lazy) variant
    fun readFileOrFolder(path: Path): Stream<FileStatus> {
        val fileStatus = fs.getFileStatus(path)
        log.trace("Process {}", fileStatus)

        return when {
            fileStatus.isDirectory -> Arrays.stream(fs.listStatus(path))
            fileStatus.isFile -> Stream.of(fileStatus)
            else -> throw IllegalStateException("Unknown FileStatus: $fileStatus")
        }
    }

    fun getFileStatusesRecursiveStream(path: Path): Stream<FileStatus> {
        return readFileOrFolder(path).flatMap { fileStatus -> if (fileStatus.isDirectory) getFileStatusesRecursiveStream(fileStatus.path) else Stream.of(fileStatus) }
    }

    // FJP (parallel) variant
    fun getFileStatusesRecursive(path: Path): List<FileStatus> {
        return fjp.invoke(RecursiveFileStatusTask(fs, fs.getFileStatus(path)))
    }

    private class RecursiveFileStatusTask(
        private val fs: FileSystem,
        private val fileStatus: FileStatus
    ) : RecursiveTask<List<FileStatus>>() {

        override fun compute(): List<FileStatus> {
            log.trace("Process {}", fileStatus)

            if (fileStatus.isDirectory) {
                log.debug { "Process dir ${fileStatus.path}" }
            }

            return when {
                fileStatus.isDirectory -> {
                    val subTasks: MutableList<RecursiveFileStatusTask> = LinkedList<RecursiveFileStatusTask>()

                    for (child in fs.listStatus(fileStatus.path)) {
                        val task = RecursiveFileStatusTask(fs, child)
                        task.fork()
                        subTasks.add(task)
                    }

                    subTasks.flatMap { it.join() }
                }
                fileStatus.isFile -> listOf(fileStatus)
                else -> throw IllegalStateException("Unknown FileStatus: $fileStatus")
            }
        }
    }
}
