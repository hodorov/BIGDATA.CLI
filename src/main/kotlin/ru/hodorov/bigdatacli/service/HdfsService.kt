package ru.hodorov.bigdatacli.service

import mu.KotlinLogging
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveTask

private val log = KotlinLogging.logger {  }

@Service
class HdfsService(
    private val fs: FileSystem,
    private val fjp: ForkJoinPool
) {
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
