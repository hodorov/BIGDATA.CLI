package ru.hodorov.bigdatacli.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import ru.hodorov.bigdatacli.utils.HDFSUtils

private val configuration = lazy {
    val newConf = Configuration()
    newConf["fs.file.impl"] = LocalFileSystem::class.java.name
    newConf
}

@Component("hdfs")
class HDFS(
    @Value("\${hdfs.timeout}") val hdfsTimeout: String
) : FS() {

    override fun getFsInstance() = HDFSUtils.getHdfs(conf.url ?: error("URL not defined"), hdfsTimeout)
}
