package ru.hodorov.bigdatacli.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.net.URI

@Component
class FsContext(
    @Value("\${hdfs.workDir}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val workDir: String,
    fs: FileSystem
) {
    // To extract full qualified name (with nameservice)
    var currentUri: URI = fs.getFileStatus(Path(workDir)).path.toUri()
}
