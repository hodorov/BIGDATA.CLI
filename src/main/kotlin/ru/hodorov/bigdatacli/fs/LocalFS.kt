package ru.hodorov.bigdatacli.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.LocalFileSystem
import org.springframework.stereotype.Component

private val fsConf by lazy {
    val newConf = Configuration()
    newConf["fs.file.impl"] = LocalFileSystem::class.java.name
    newConf
}

@Component("local")
class LocalFS: FS() {
    override fun getFsInstance(): FileSystem = FileSystem.get(fsConf)
}
