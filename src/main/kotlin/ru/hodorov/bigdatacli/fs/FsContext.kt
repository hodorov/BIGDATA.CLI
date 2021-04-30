package ru.hodorov.bigdatacli.fs

import org.springframework.stereotype.Component
import ru.hodorov.bigdatacli.utils.ApplicationContextUtils

@Component
class FsContext(
    private val fsConfigurationProperties: FsConfigurationProperties,
    private val fsImls: Map<String, FS>,
    private val applicationContextUtils: ApplicationContextUtils
) {

    val fsInstances = HashMap<String, FS>()

    final lateinit var activeFsName: String
        private set

    final lateinit var activeFs: FS
        private set

    var currentUri
        get() = activeFs.currentUri
        set(value) {
            activeFs.currentUri = value
        }
    val fs
        get() = activeFs.fs

    init {
        switch(fsConfigurationProperties.default)
    }

    final fun switch(name: String) {
        synchronized(this) {
            activeFs = getOrCreateFsInstance(name, fsConfigurationProperties.list[name] ?: error("FS with name $name not found"))
            activeFsName = name
        }
    }

    private fun getOrCreateFsInstance(name: String, conf: FsInstanceConfiguration) = fsInstances.getOrPut(name) {
        val impl = fsImls[conf.type] ?: error("FS with type ${conf.type} not found")
        val instance = applicationContextUtils.recreateBean(impl)
        instance.conf = conf
        instance.name = name
        instance.connect()
        return@getOrPut instance
    }
}
