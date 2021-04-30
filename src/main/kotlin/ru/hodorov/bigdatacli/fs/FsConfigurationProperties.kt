package ru.hodorov.bigdatacli.fs

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "fs")
class FsConfigurationProperties {
    lateinit var default: String
    val list = HashMap<String, FsInstanceConfiguration>()
}

class FsInstanceConfiguration {
    lateinit var type: String
    lateinit var url: String
    var workdir: String? = null
}
