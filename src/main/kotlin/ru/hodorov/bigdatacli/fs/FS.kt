package ru.hodorov.bigdatacli.fs

import mu.KotlinLogging
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import kotlin.properties.Delegates


private val log = KotlinLogging.logger { }

abstract class FS {

    private var _fs: FileSystem? = null
    private var _currentUri: URI? = null

    var status by Delegates.observable(FSStatus.CREATED) { _, oldValue, newValue ->
        log.info { "FS $name $oldValue->$newValue" }
    }

    val fs: FileSystem
        get() {
            synchronized(this) {
                if (status == FSStatus.DISCONNECTED) {
                    connect()
                }

                return _fs!!
            }
        }

    var currentUri: URI
        get() {
            synchronized(this) {
                check(status == FSStatus.CONNECTED) { "Wrong current FS status $status" }

                return _currentUri!!
            }
        }
        set(value) {
            synchronized(this) {
                check(status == FSStatus.CONNECTED) { "Wrong current FS status $status" }
                _currentUri = value
            }
        }

    // Init by FsContext
    lateinit var conf: FsInstanceConfiguration
    lateinit var name: String

    protected abstract fun getFsInstance(): FileSystem

    fun connect() {
        synchronized(this) {
            check(status == FSStatus.CONNECTED || status == FSStatus.CREATED) { "Wrong current FS status $status" }
            _fs = getFsInstance()
            _currentUri = _fs!!.getFileStatus(Path(conf.workdir)).path.toUri()
            status = FSStatus.CONNECTED
        }
    }

    fun disconnect() {
        synchronized(this) {
            check(status == FSStatus.CONNECTED) { "Wrong current FS status $status" }
            _fs!!.close()
            _fs = null
            _currentUri = null
            status = FSStatus.DISCONNECTED
        }
    }

    fun reconnect() {
        synchronized(this) {
            if (status != FSStatus.DISCONNECTED) {
                disconnect()
            }
            connect()
        }
    }
}

enum class FSStatus {
    CREATED,
    DISCONNECTED,
    CONNECTED
}
