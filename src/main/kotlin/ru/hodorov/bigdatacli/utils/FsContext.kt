package ru.hodorov.bigdatacli.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.net.URI

@Component
class FsContext(
    @Value("\${hdfs.workDir}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val hdfsWorkDir: String,
    @Value("\${local.workDir}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val localWorkDir: String,
    @Qualifier("hdfs") val hdfs: FileSystem,
    @Qualifier("local") val localFs: FileSystem
) {
    enum class ACTIVE_FS {
        HDFS,
        LOCAL
    }

    var activeFs: ACTIVE_FS = ACTIVE_FS.HDFS
    val fs: FileSystem
        get() = when (activeFs) {
            ACTIVE_FS.HDFS -> hdfs
            ACTIVE_FS.LOCAL -> localFs
        }

    var hdfsCurrentUri: URI = hdfs.getFileStatus(Path(hdfsWorkDir)).path.toUri()
    var localCurrentUri: URI = localFs.getFileStatus(Path(localWorkDir)).path.toUri()

    var currentUri: URI
        get() = when (activeFs) {
            ACTIVE_FS.HDFS -> hdfsCurrentUri
            ACTIVE_FS.LOCAL -> localCurrentUri
        }
        set(value) {
            when (activeFs) {
                ACTIVE_FS.HDFS -> hdfsCurrentUri = value
                ACTIVE_FS.LOCAL -> localCurrentUri = value
            }
        }
}
